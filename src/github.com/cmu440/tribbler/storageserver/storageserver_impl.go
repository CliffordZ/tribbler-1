package storageserver

import (
	"container/list"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"
)

type storageServer struct {
	nodeID         uint32
	numNodes       int
	servers        []storagerpc.Node
	numRegistered  int
	newServer      chan storagerpc.Node
	newServerReply chan storagerpc.RegisterReply

	itemDatastore map[string]string
	listDatastore map[string]*list.List
}

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
		masterServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
		args := &storagerpc.RegisterArgs{
			ServerInfo: *node,
		}
		reply := &storagerpc.RegisterReply{}
		for {
			//Make register server rpc call
			if err = masterServer.Call("StorageServer.RegisterServer", args, reply); err != nil {
				return nil, err
			}
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
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = value
		//TODO: Implement Leasing
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ls, ok := ss.listDatastore[args.Key]

	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		values := make([]string, ls.Len())

		index := 0
		for e := ls.Front(); e != nil; e = e.Next() {
			values[index] = e.Value.(string)
			index++
		}

		reply.Status = storagerpc.OK
		reply.Value = values
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.itemDatastore[args.Key] = args.Value

	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ls, ok := ss.listDatastore[args.Key]

	// Create new list if one doesn't already exist
	if !ok {
		ls = list.New()
	}

	// Check if item is already in the list
	for e := ls.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)

		if args.Value == value {
			reply.Status = storagerpc.ItemExists
			return nil
		}
	}

	ls.PushBack(args.Value)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectStorageServer(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ls, ok := ss.listDatastore[args.Key]

	// If no list, reply with item not found
	if !ok {
		reply.Status = storagerpc.ItemNotFound
		return nil
	}

	for e := ls.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)

		if args.Value == value {
			ls.Remove(e)
			reply.Status = storagerpc.OK
			return nil
		}
	}

	// If item not found, reply with item not found
	reply.Status = storagerpc.ItemNotFound
	return nil
}
