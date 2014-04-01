package tribserver

import (
	"errors"

	"net/rpc"
	"github.com/cmu440/tribbler/rpc/tribrpc"
        "time"
        "encoding/json"
)

const (
  LeaseMode leaseMode = Never
  subscriptionToken string = ":s"
  tribbleToken string = ":t"
  userToken = ":u"
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
    libstore: libstore,
    listener: listener,
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
  _, err := ts.libstore.Get(args.UserID+userToken)
  if err == nil {
    reply.Status = Exists
    return nil
  }
  err := ts.libstore.Put(args.UserID+userToken, "")
  if err == nil {
    reply.Status = OK
  }
  return err
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
  _, err := ts.libstore.Get(args.UserID+userToken)
  if err != nil {
    reply.Status = NoSuchUser
    return err
  }
  _, err := ts.libstore.Get(args.TargetUserID+userToken)
  if err != nil {
    reply.Status = NoSuchTargetUser
    return err
  }
  key := args.UserID+subscriptionToken
  err := ts.libstore.AppendToList(key, args.TargetUserID)
  if err == nil {
    reply.Status = OK
  }
  return err
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
  _, err := ts.libstore.Get(args.UserID+userToken)
  if err != nil {
    reply.Status = NoSuchUser
    return err
  }
  _, err := ts.libstore.Get(args.TargetUserID+userToken)
  if err != nil {
    reply.Status = NoSuchTargetUser
    return err
  }
  key := args.UserID+subscriptionToken
  err := ts.libstore.RemoveFromList(key, args.TargetUserID)
  if err == nil {
    reply.Status = OK
  }
  return err
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
  _, err := ts.libstore.Get(args.UserID+userToken)
  if err != nil {
    reply.Status = NoSuchUser
    return err
  }
  key := args.UserID+subscriptionToken
  subscriptions, err := ts.libstore.GetList(key)
  if err == nil {
    reply.Status = OK
    reply.UserIDs = subscriptions
  }
  return err
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
  _, err := ts.libstore.Get(args.UserID+userToken)
  if err != nil {
    reply.Status = NoSuchUser
    return err
  }
  tribbleID := args.UserID + tribbleToken + ":" + time.Now().UnixNano()
  userTribbleID := args.UserID+tribbleToken

  // Marshalled tribble
  tribble := &Tribble {
    UserID : args.UserID,
    Posted : time.Now(),
    Contents : args.Contents,
  }
  marshalledTribble, _ := string(json.Marshal(tribble)[:])

  // Store tribble
  err := ts.libstore.AppendToList(tribbleID, marshalledTribble)
  if err != nil {
    return err
  }

  //store tribble ID into user list
  err := ts.libstore.AppendToList(userTribbleID, tribbleID)
  if err == nil {
    reply.Status == OK
  }
  return err
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
