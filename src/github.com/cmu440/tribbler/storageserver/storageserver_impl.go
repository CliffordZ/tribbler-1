package storageserver

import (
	"container/list"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const leaseSeconds = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

type leaseTracker struct {
	hostport  string
	grantedAt time.Time
}

type storageServer struct {
	nodeID         uint32
	numNodes       int
	servers        []storagerpc.Node
	numRegistered  int
	newServer      chan storagerpc.Node
	newServerReply chan storagerpc.RegisterReply

	itemDatastore map[string]string
	listDatastore map[string]*list.List

	leaseMap      map[string]*list.List  // Keep track of which libstores have leases for a given key
	grantMap      map[string]bool        // Keep track of if a lease can be granted for a given key
	libstoreConns map[string]*rpc.Client // Cache http connections to libstore clients

	// mutexes
	itemLock  *sync.Mutex
	listLock  *sync.Mutex
	leaseLock *sync.Mutex
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

// Confirm that request has been routed to correct storage server
func isCorrectStorageServer(ss *storageServer, key string) bool {
	var matchedNodeID, minNodeID uint32 = math.MaxUint32, math.MaxUint32
	hash := libstore.StoreHash(strings.Split(key, ":")[0])

	for _, server := range ss.servers {
		if server.NodeID >= hash && server.NodeID < matchedNodeID {
			matchedNodeID = server.NodeID
		} else if server.NodeID < minNodeID {
			minNodeID = server.NodeID
		}
	}

	return (matchedNodeID == math.MaxUint32 && ss.nodeID == minNodeID) || ss.nodeID == matchedNodeID
}

// Create and cache libstore connection
func cacheLibstoreConnection(ss *storageServer, hostport string) {
	_, ok := ss.libstoreConns[hostport]

	if !ok {
		libstore, _ := rpc.DialHTTP("tcp", hostport)
		ss.libstoreConns[hostport] = libstore
	}
}

// Determine if lease can currently be granted for a given key
func canGrantLease(ss *storageServer, key string) bool {
	grantable, ok := ss.grantMap[key]
	return !ok || grantable
}

// Handle lease request in Get or GetList
func handleLeaseRequest(ss *storageServer, key, hostport string) storagerpc.Lease {
	cacheLibstoreConnection(ss, hostport)

	lease := storagerpc.Lease{
		Granted:      false,
		ValidSeconds: 0,
	}
	if canGrantLease(ss, key) {
		lease.Granted = true
		lease.ValidSeconds = storagerpc.LeaseSeconds

		// Add libstore hostport to leaseMap
		leases, ok := ss.leaseMap[key]
		if !ok {
			leases = list.New()
			ss.leaseMap[key] = leases
		}
		tracker := &leaseTracker{
			hostport:  hostport,
			grantedAt: time.Now(),
		}
		leases.PushBack(tracker)
	}

	return lease
}

// Revoke and block leases from being granted for a given key
func revokeAndBlockLeases(ss *storageServer, key string) error {
	// Prevent new leases from being granted
	ss.grantMap[key] = false

	// Revoke all leases and wait for responses
	leases, ok := ss.leaseMap[key]

	if ok {
		// For each lease, we send the RevokeLease rpc call to it
		for e := leases.Front(); e != nil; e = e.Next() {
			leaseTracker := e.Value.(*leaseTracker)

			if !leaseExpired(leaseTracker.grantedAt) {
				args := &storagerpc.RevokeLeaseArgs{
					Key: key,
				}
				reply := &storagerpc.RevokeLeaseReply{}
				asyncCall := ss.libstoreConns[leaseTracker.hostport].Go("LeaseCallbacks.RevokeLease", args, reply, nil)
				timeoutSeconds := leaseSeconds - time.Since(leaseTracker.grantedAt).Seconds()
				select {
				case <-asyncCall.Done:
					break
				case <-time.After(time.Duration(timeoutSeconds) * time.Second):
					break
				}
			}

			leases.Remove(e)
		}
	}

	return nil
}

// Checks if a lease has expired
func leaseExpired(grantedAt time.Time) bool {
	return time.Since(grantedAt).Seconds() > (storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds)
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	hostPort := "localhost:" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	ss := &storageServer{
		nodeID:         nodeID,
		numNodes:       numNodes,
		servers:        make([]storagerpc.Node, numNodes),
		numRegistered:  1,
		newServer:      make(chan storagerpc.Node),
		newServerReply: make(chan storagerpc.RegisterReply),
		itemDatastore:  make(map[string]string),
		listDatastore:  make(map[string]*list.List),
		leaseMap:       make(map[string]*list.List),
		grantMap:       make(map[string]bool),
		libstoreConns:  make(map[string]*rpc.Client),
		itemLock:       &sync.Mutex{},
		listLock:       &sync.Mutex{},
		leaseLock:      &sync.Mutex{},
	}

	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}

	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	node := &storagerpc.Node{
		HostPort: "localhost:" + strconv.Itoa(port),
		NodeID:   nodeID,
	}

	//If not master server, register slave
	if masterServerHostPort != "" {
		//Setup connection with master server
		masterServer, _ := rpc.DialHTTP("tcp", masterServerHostPort)
		args := &storagerpc.RegisterArgs{
			ServerInfo: *node,
		}
		reply := &storagerpc.RegisterReply{}
		for {
			//Make register server rpc call
			masterServer.Call("StorageServer.RegisterServer", args, reply)
			//If the master server is not ready, sleep and keep pinging server
			if reply.Status == storagerpc.NotReady {
				time.Sleep(time.Second)
			} else {
				ss.servers = reply.Servers
				ss.numRegistered = numNodes
				return ss, nil
			}
		}
	} else {
		//Put master server in servers list
		ss.servers[0] = *node
		//Loop while not all nodes have registered yet
		for ss.numRegistered < numNodes {
			//Get new server from channel in RegisterServer
			newServer := <-ss.newServer
			//Find if server has already registered
			alreadyRegistered := false
			for i := range ss.servers {
				if ss.servers[i] == newServer {
					alreadyRegistered = true
				}
			}
			//If server has not already registered, add to list of registered servers
			if !alreadyRegistered {
				ss.servers[ss.numRegistered] = newServer
				ss.numRegistered++
			}
			//If not all servers registered, reply with NotReady
			if ss.numRegistered < ss.numNodes {
				reply := &storagerpc.RegisterReply{
					Status:  storagerpc.NotReady,
					Servers: nil,
				}
				ss.newServerReply <- *reply
			} else {
				//If all serversRegistered, reply with OK
				reply := &storagerpc.RegisterReply{
					Status:  storagerpc.OK,
					Servers: ss.servers,
				}
				ss.newServerReply <- *reply
			}
		}
	}
	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if ss.numNodes > ss.numRegistered {
		ss.newServer <- args.ServerInfo
		*reply = <-ss.newServerReply
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	//If not all nodes have registered, reply NotReady
	if ss.numNodes > ss.numRegistered {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	} else {
		//If OK reply wtih servers list
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	value, ok := ss.itemDatastore[args.Key]

	if !ok {
		LOGE.Println("Key not found get")
		reply.Status = storagerpc.KeyNotFound
	} else {
		if args.WantLease {
			reply.Lease = handleLeaseRequest(ss, args.Key, args.HostPort)
		}

		reply.Status = storagerpc.OK
		reply.Value = value
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.listLock.Lock()
	ls, ok := ss.listDatastore[args.Key]

	if !ok {
		LOGE.Println("Key not found getlist")
		reply.Status = storagerpc.KeyNotFound
	} else {
		values := make([]string, ls.Len())

		index := 0
		for e := ls.Front(); e != nil; e = e.Next() {
			values[index] = e.Value.(string)
			index++
		}

		if args.WantLease {
			reply.Lease = handleLeaseRequest(ss, args.Key, args.HostPort)
		}
		reply.Status = storagerpc.OK
		reply.Value = values
	}
	ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	err := revokeAndBlockLeases(ss, args.Key)
	if err != nil {
		LOGE.Println("FUCK")
		return err
	}

	// Put value into datastore
	ss.itemDatastore[args.Key] = args.Value

	// Allow leases to be granted again
	ss.grantMap[args.Key] = true

	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	err := revokeAndBlockLeases(ss, args.Key)
	if err != nil {
		return err
	}

	ss.listLock.Lock()

	ls, ok := ss.listDatastore[args.Key]

	// Create new list if one doesn't already exist
	if !ok {
		ls = list.New()
		ss.listDatastore[args.Key] = ls
	}

	// Check if item is already in the list
	for e := ls.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)

		if args.Value == value {
			// Allow leases to be granted again
			ss.grantMap[args.Key] = true
			reply.Status = storagerpc.ItemExists
			ss.listLock.Unlock()
			return nil
		}
	}

	ls.PushBack(args.Value)

	// Allow leases to be granted again
	ss.grantMap[args.Key] = true
	reply.Status = storagerpc.OK
	ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	err := revokeAndBlockLeases(ss, args.Key)
	if err != nil {
		return err
	}

	ss.listLock.Lock()
	ls, ok := ss.listDatastore[args.Key]

	// If no list, reply with item not found
	if !ok {
		// Allow leases to be granted again
		ss.grantMap[args.Key] = true

		reply.Status = storagerpc.ItemNotFound
		ss.listLock.Unlock()
		return nil
	}

	for e := ls.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)

		if args.Value == value {
			ls.Remove(e)

			// Allow leases to be granted again
			ss.grantMap[args.Key] = true
			reply.Status = storagerpc.OK
			ss.listLock.Unlock()
			return nil
		}
	}

	// Allow leases to be granted again
	ss.grantMap[args.Key] = true

	// If item not found, reply with item not found
	reply.Status = storagerpc.ItemNotFound
	ss.listLock.Unlock()
	return nil
}
