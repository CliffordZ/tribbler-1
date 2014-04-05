package libstore

import (
	"container/list"
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

type leaseItem struct {
  value string
  inserted time.Time
  validSeconds int
}

type leaseList struct {
  value []string
  inserted time.Time
  validSeconds int
}

type libstore struct {
	servers  map[uint32]*rpc.Client // Map from NodeID to storage server connection
	mode     LeaseMode
	hostport string

	leaseManager map[string]*list.List // Keep track of requests from the last QueryCacheSeconds seconds
	itemCache    map[string]*leaseItem
	listCache    map[string]*leaseList
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

// Look for matching node ID, also keeping track of min node id in case of wrap around
func determineNode(ls *libstore, key string) *rpc.Client {
	var matchedNodeID, minNodeID uint32 = math.MaxUint32, math.MaxUint32
	hash := StoreHash(strings.Split(key, ":")[0])

	for nodeID := range ls.servers {
		if nodeID >= hash && nodeID < matchedNodeID {
			matchedNodeID = nodeID
		} else if nodeID < minNodeID {
			minNodeID = nodeID
		}
	}

	if matchedNodeID == math.MaxUint32 {
		return ls.servers[minNodeID]
	}
	return ls.servers[matchedNodeID]
}

func requiresLease(ls *libstore, key string) bool {
  if ls.mode == Never {
    return false
  } else if ls.mode == Always {
    return true
  }
  
  // Fetch or create list of past requests corresponding to key
	requests, ok := ls.leaseManager[key]
  if !ok {
    requests = list.New()
    ls.leaseManager[key] = requests
  }

  currentTime := time.Now()

  // Remove all requests that are older than QueryCacheSeconds
	for e := requests.Front(); e != nil; e = e.Next() {
		queryTime := e.Value.(time.Time)
    if currentTime.Sub(queryTime).Seconds() > storagerpc.QueryCacheSeconds {
      requests.Remove(e)
    }
  }

  // Add new request
  requests.PushBack(currentTime)

  // Check if there are more than QueryCacheThresh queries
  return requests.Len() >= storagerpc.QueryCacheThresh
}

// Check if there's a valid leased cache element for key
func validLeaseItem(ls *libstore, key string) bool {
  item, ok := ls.itemCache[key]

  return ok && int(time.Since(item.inserted).Seconds()) < item.validSeconds
}
func validLeaseList(ls *libstore, key string) bool {
  item, ok := ls.listCache[key]

  return ok && int(time.Since(item.inserted).Seconds()) < item.validSeconds
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
		servers:      servers,
		mode:         mode,
		hostport:     myHostPort,
		leaseManager: make(map[string]*list.List),
		itemCache:    make(map[string]*leaseItem),
		listCache:    make(map[string]*leaseList),
	}
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	if err != nil {
		return nil, err
	}
	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	routedServer := determineNode(ls, key)
	wantLease := requiresLease(ls, key)
  currentTime := time.Now()

  // If wantLease and there's a valid cached element, return cached element
  if wantLease && validLeaseItem(ls, key) {
    return ls.itemCache[key].value, nil
  }

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
    // Cache value if WantLease was set true
    if wantLease && reply.Lease.Granted {
      ls.itemCache[key] = &leaseItem{
        value: reply.Value,
        inserted: currentTime,
        validSeconds: reply.Lease.ValidSeconds,
      }
    }

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
	wantLease := requiresLease(ls, key)
  currentTime := time.Now()

  // If wantLease and there's a valid cached element, return cached element
  if wantLease && validLeaseList(ls, key) {
    return ls.listCache[key].value, nil
  }

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
    // Cache value if WantLease was set true
    if wantLease && reply.Lease.Granted {
      ls.listCache[key] = &leaseList{
        value: reply.Value,
        inserted: currentTime,
        validSeconds: reply.Lease.ValidSeconds,
      }
    }

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
  _, ok := ls.itemCache[args.Key]

  if ok {
    delete(ls.itemCache, args.Key)
    reply.Status = storagerpc.OK
    return nil
  }

  _, ok = ls.listCache[args.Key]
  if ok {
    delete(ls.listCache, args.Key)
    reply.Status = storagerpc.OK
  } else {
    reply.Status = storagerpc.KeyNotFound
  }

  return nil
}
