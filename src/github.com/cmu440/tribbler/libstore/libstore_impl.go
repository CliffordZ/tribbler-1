package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
)

const (
	maxAttempts int = 5
)

type libstore struct {
	servers []storagerpc.Node
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	// Conncet to master server
	masterServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	// Fetch list of servers, repeating up to retryAttempts times
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	for i := 0; i < maxAttempts; i++ {
		if err = masterServer.Call("TribServer.CreateUser", args, reply); err != nil {
			return nil, err
		}

		// Sleep and retry if server not ready
		if reply.Status == storagerpc.NotReady {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	// Return error if storage server connection failed after maxAttempts attempts
	if reply.Status == storagerpc.NotReady {
		return nil, errors.New("Unable to connect to ServerStorage")
	}

	// Create libstore and register it for RPC callbacks
	libstore := &libstore{
		servers: reply.Servers,
	}
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	if err != nil {
		return nil, err
	}
	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	return "", errors.New("not implemented")
}

func (ls *libstore) Put(key, value string) error {
	return errors.New("not implemented")
}

func (ls *libstore) GetList(key string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) AppendToList(key, newItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
