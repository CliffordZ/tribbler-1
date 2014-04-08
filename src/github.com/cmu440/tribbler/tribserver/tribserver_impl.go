package tribserver

import (
	"container/list"
	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"log"
	"os"
)

const (
	leaseMode         libstore.LeaseMode = libstore.Normal
	subscriptionToken string             = ":s"
	tribbleToken      string             = ":t"
	userToken         string             = ":u"
	maxTribbles       int                = 100
)

type tribServer struct {
	libstore *libstore.Libstore
	listener *net.Listener
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	// Create libstore
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, leaseMode)
	if err != nil {
		return nil, err
	}

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Create TribServer with libstore and listener
	tribServer := &tribServer{
		libstore: &libstore,
		listener: &listener,
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	err = (*ts.libstore).Put(args.UserID+userToken, "")
	if err == nil {
		reply.Status = tribrpc.OK
	}
	return err
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = (*ts.libstore).Get(args.TargetUserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	key := args.UserID + subscriptionToken
	err = (*ts.libstore).AppendToList(key, args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.Exists
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = (*ts.libstore).Get(args.TargetUserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	key := args.UserID + subscriptionToken
	err = (*ts.libstore).RemoveFromList(key, args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.NoSuchTargetUser
	}
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	key := args.UserID + subscriptionToken
	subscriptions, err := (*ts.libstore).GetList(key)
	if err == nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = subscriptions
	} else {
		reply.Status = tribrpc.OK
		reply.UserIDs = []string{}
	}
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	currentTime := time.Now()
	tribbleID := args.UserID + tribbleToken + ":" + strconv.FormatInt(currentTime.UnixNano(), 10)
	userTribbleID := args.UserID + tribbleToken

	// Marshalled tribble
	tribble := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   currentTime,
		Contents: args.Contents,
	}
	marshalledBytes, _ := json.Marshal(tribble)
	marshalledTribble := string(marshalledBytes)

	// Store tribble
	err = (*ts.libstore).Put(tribbleID, marshalledTribble)
	if err != nil {
		return err
	}

	//store tribble ID into user list
	err = (*ts.libstore).AppendToList(userTribbleID, tribbleID)
	if err == nil {
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userTribbleID := args.UserID + tribbleToken

	tribbleIDs, err := (*ts.libstore).GetList(userTribbleID)
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}

	var tribbles [maxTribbles]tribrpc.Tribble
	tribbleCount := 0

	// Fetch up to maxTribbles tribbles for the user
	for i := len(tribbleIDs) - 1; i >= 0 && tribbleCount < maxTribbles; i-- {
		tribble := &tribrpc.Tribble{}
		marshalledTribble, err := (*ts.libstore).Get(tribbleIDs[i])
		if err != nil {
			return err
		}

		json.Unmarshal([]byte(marshalledTribble), tribble)
		tribbles[tribbleCount] = *tribble

		tribbleCount++
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles[:tribbleCount]
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// Check that user exists
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Get user's subscriptions
	key := args.UserID + subscriptionToken
	subscriptions, err := (*ts.libstore).GetList(key)

	// Return if user has no subscriptions
	if len(subscriptions) == 0 {
		reply.Status = tribrpc.OK
		reply.Tribbles = nil
		return nil
	}

	// Get tribbles from all subscriptions
	allTribbles := list.New()
	for i := range subscriptions {
		targetUserID := subscriptions[i] + tribbleToken

		tribbleIDs, err := (*ts.libstore).GetList(targetUserID)
		if err == nil && len(tribbleIDs) > 0 {
			allTribbles.PushBack(tribbleIDs)
		}
	}

	// Grab 100 most recent tribbles
	var tribbles [maxTribbles]tribrpc.Tribble
	var i int
	for i = 0; i < maxTribbles && allTribbles.Len() > 0; i++ {
		// Find most recent Tribble
		mostRecentTribbles := allTribbles.Front()
		firstTribbles := mostRecentTribbles.Value.([]string)
		mostRecentTribbleID := firstTribbles[len(firstTribbles)-1]
		mostRecentTime := strings.Split(mostRecentTribbleID, ":")[2]
		for e := allTribbles.Front().Next(); e != nil; e = e.Next() {
			subsTribbles := e.Value.([]string)
			tribbleID := subsTribbles[len(subsTribbles)-1]
			tribbleTime := strings.Split(tribbleID, ":")[2]

			// String comparison like this works apparently
			if tribbleTime > mostRecentTime {
				mostRecentTribbles = e
				mostRecentTribbleID = tribbleID
				mostRecentTime = tribbleTime
			}
		}

		// Unmarshall and store the tribble
		tribble := &tribrpc.Tribble{}
		marshalledTribble, err := (*ts.libstore).Get(mostRecentTribbleID)
		if err != nil {
			return err
		}

		json.Unmarshal([]byte(marshalledTribble), tribble)
		tribbles[i] = *tribble

		// Remove the tribble from list
		removedTribbles := mostRecentTribbles.Value.([]string)
		removedTribbles = removedTribbles[:len(removedTribbles)-1]
		if len(removedTribbles) > 0 {
			allTribbles.InsertBefore(removedTribbles, mostRecentTribbles)
		}
		allTribbles.Remove(mostRecentTribbles)
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles[:i]
	return nil
}
