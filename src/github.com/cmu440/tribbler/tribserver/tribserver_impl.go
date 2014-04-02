package tribserver

import (
        "container/list"
        "strings"
	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

const (
	leaseMode         libstore.LeaseMode = libstore.Never
	subscriptionToken string             = ":s"
	tribbleToken      string             = ":t"
	userToken         string             = ":u"
	maxTribbles       int                = 100
)

type tribServer struct {
	libstore *libstore.Libstore
	listener *net.Listener
}

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
		return err
	}
	_, err = (*ts.libstore).Get(args.TargetUserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}
	key := args.UserID + subscriptionToken
	err = (*ts.libstore).AppendToList(key, args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
	}
	return err
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}
	_, err = (*ts.libstore).Get(args.TargetUserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}
	key := args.UserID + subscriptionToken
	err = (*ts.libstore).RemoveFromList(key, args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
	}
	return err
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}
	key := args.UserID + subscriptionToken
	subscriptions, err := (*ts.libstore).GetList(key)
	if err == nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = subscriptions
	}
	return err
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}
	tribbleID := args.UserID + tribbleToken + ":" + string(time.Now().UnixNano())
	userTribbleID := args.UserID + tribbleToken

	// Marshalled tribble
	tribble := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   time.Now(),
		Contents: args.Contents,
	}
	marshalledBytes, _ := json.Marshal(tribble)
	marshalledTribble := string(marshalledBytes[:])

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
	return err
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_, err := (*ts.libstore).Get(args.UserID + userToken)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}
	userTribbleID := args.UserID + tribbleToken

	tribbleIDs, err := (*ts.libstore).GetList(userTribbleID)
	if err != nil {
		return err
	}

	var tribbles [maxTribbles]tribrpc.Tribble

	// Fetch first 100 tribbles for the user
	for i := 0; i < maxTribbles && i < len(tribbleIDs); i++ {
		tribble := tribrpc.Tribble{}
		marshalledTribble, err := (*ts.libstore).Get(tribbleIDs[i])
		if err != nil {
			return err
		}

		json.Unmarshal([]byte(marshalledTribble), tribble)
		tribbles[i] = tribble
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles[:]
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
        // check that user exists
	_, err := (*ts.libstore).Get(args.UserID + userToken)
        if err != nil {
          reply.Status = tribrpc.NoSuchUser
          return err
        }
        
        //get user's subscriptions
	key := args.UserID + subscriptionToken
	subscriptions, err := (*ts.libstore).GetList(key)

        //if user has no subscriptions
        if len(subscriptions) == 0 {
          reply.Status = tribrpc.OK
          reply.Tribbles = nil
          return nil
        }
        
        //get tribbles from all subscriptions
        allTribbles := list.New()
        for i := range subscriptions {
          targetUserID :=  subscriptions[i] + tribbleToken

          tribbleIDs, err :=  (*ts.libstore).GetList(targetUserID)
          if err != nil {
            return err
          }
          allTribbles.PushBack(tribbleIDs)
        }

        //grab 100 most recent tribbles
        var tribbles [maxTribbles]tribrpc.Tribble
        for i := 0; i < maxTribbles && allTribbles.Len() > 0; i++ {

                //get most recent Tribble
                mostRecentTribbleID := allTribbles.Front().Value.([]string)[0]
                for e := allTribbles.Front(); e != nil; e = e.Next()  {
                  subsTribbles := e.Value.([]string)
                  mostRecentTime := strings.Split(mostRecentTribbleID,":")[2]
                  tempTime := strings.Split(subsTribbles[0],":")[2]

                  if tempTime < mostRecentTime{
                    mostRecentTribbleID = subsTribbles[0]

                    //remove tribble from list
                    subsTribbles = subsTribbles[1:]
                    if len(subsTribbles) > 0 {
                      allTribbles.InsertBefore(subsTribbles,e)
                    }
                    allTribbles.Remove(e)
                  }
                }
                
                tribble := tribrpc.Tribble{}
		marshalledTribble, err := (*ts.libstore).Get(mostRecentTribbleID)
		if err != nil {
			return err
		}

		json.Unmarshal([]byte(marshalledTribble), tribble)
		tribbles[i] = tribble
	}
        reply.Status = tribrpc.OK
        reply.Tribbles = tribbles[:]
        return nil
}
