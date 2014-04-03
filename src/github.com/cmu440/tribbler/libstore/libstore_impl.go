package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"net/rpc"
	"strings"
	"time"

	"log"
	"os"
	"strconv"
)

const (
	maxAttempts int = 5
)

type libstore struct {
	servers  map[uint32]*rpc.Client // Map from NodeID to storage server connection
	mode     LeaseMode
	hostport string
	nodeID   uint32 // Used until Part 2 is complete
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

func determineNode(ls *libstore, key string) *rpc.Client {
	var minNodeID uint32 = math.MaxUint32
	hash := StoreHash(strings.Split(key, ":")[0])

	for nodeID := range ls.servers {
		if nodeID >= hash && nodeID < minNodeID {
			minNodeID = nodeID
		}
	}
	return ls.servers[minNodeID]
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
	// Connect to master server
	masterServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	// Fetch list of servers, repeating up to retryAttempts times
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	for i := 0; i < maxAttempts; i++ {
		if err = masterServer.Call("StorageServer.GetServers", args, reply); err != nil {
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
		return nil, errors.New("Unable to connect to StorageServer")
	}

	// Allocate map for node id to server connection
	servers := make(map[uint32]*rpc.Client)

	for _, node := range reply.Servers {
		server, err := rpc.DialHTTP("tcp", node.HostPort)
		if err != nil {
			return nil, err
		}

		servers[node.NodeID] = server
	}

	// Create libstore and register it for RPC callbacks
	libstore := &libstore{
		servers:  servers,
		mode:     mode,
		hostport: myHostPort,
		nodeID:   reply.Servers[0].NodeID,
	}
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	if err != nil {
		return nil, err
	}
	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	routedServer := determineNode(ls, key)
	// TODO(wesley): Set appropriate lease mode as part of part 3 of libstore
	wantLease := false

	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostport,
	}
	reply := &storagerpc.GetReply{}
	if err := routedServer.Call("StorageServer.Get", args, reply); err != nil {
		return "", err
	}

	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return "", errors.New("Error in Get request to StorageServer. Status: " + strconv.Itoa(int(reply.Status)))
	}
}

func (ls *libstore) Put(key, value string) error {
	routedServer := determineNode(ls, key)

	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	reply := &storagerpc.PutReply{}
	if err := routedServer.Call("StorageServer.Put", args, reply); err != nil {
		return err
	}

	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Error in Put request to StorageServer. Status: " + strconv.Itoa(int(reply.Status)))
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	routedServer := determineNode(ls, key)
	// TODO(wesley): Set appropriate lease mode as part of part 3 of libstore
	wantLease := false

	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostport,
	}
	reply := &storagerpc.GetListReply{}
	if err := routedServer.Call("StorageServer.GetList", args, reply); err != nil {
		return nil, err
	}

	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return nil, errors.New("Error in GetList request to StorageServer. Status: " + strconv.Itoa(int(reply.Status)))
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	routedServer := determineNode(ls, key)

	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	reply := &storagerpc.PutReply{}
	if err := routedServer.Call("StorageServer.RemoveFromList", args, reply); err != nil {
		return err
	}

	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Error in RemoveFromList request to StorageServer. Status: " + strconv.Itoa(int(reply.Status)))
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	routedServer := determineNode(ls, key)

	args := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	reply := &storagerpc.PutReply{}
	if err := routedServer.Call("StorageServer.AppendToList", args, reply); err != nil {
		return err
	}

	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Error in AppendToList request to StorageServer. Status: " + strconv.Itoa(int(reply.Status)))
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
