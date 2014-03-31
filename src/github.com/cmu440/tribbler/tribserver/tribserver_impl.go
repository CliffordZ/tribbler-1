package tribserver

import (
	"errors"

	"net/rpc"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

const LeaseMode leaseMode = Never

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
	return errors.New("not implemented")
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
