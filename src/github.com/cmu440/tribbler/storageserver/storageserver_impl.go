package storageserver

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
)

type storageServer struct {
	listener             *net.Listener
	masterServerHostPort string
	numNodes             int
	servers              []storagerpc.Node
	numRegistered        int
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

	storageServer := &storageServer{
		listener:             &listener,
		masterServerHostPort: masterServerHostPort,
		numNodes:             numNodes,
		servers:              make([]storagerpc.Node, numNodes),
		numRegistered:        1,
	}

	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer))
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
				storageServer.servers = reply.Servers
				storageServer.numRegistered = numNodes
				return storageServer, nil
			}
		}
	} else {
		//TODO: this seems wrong. unsure if there is a cleaner way to check if all servers registered
		storageServer.servers[0] = *node
		for {
			if storageServer.numRegistered == numNodes {
				return storageServer, nil
			} else {
				//sleep so we're not running in a tight loop?
				time.Sleep(time.Second)
			}
		}
	}
	return nil, errors.New("Should not have reached this point")
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	//Check if node is already registered
	alreadyRegistered := false
	for node := range ss.servers {
		if ss.servers[node] == args.ServerInfo {
			alreadyRegistered = true
		}
	}
	//If not already registered add to list of servers
	if !alreadyRegistered {
		ss.servers[ss.numRegistered] = args.ServerInfo
		ss.numRegistered++
	}
	//Reply NotReady if not all nodes have registered yet
	if ss.numRegistered < ss.numNodes {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
		return nil
	}
	//Reply OK and with servers list if all nodes have registered
	reply.Status = storagerpc.OK
	reply.Servers = ss.servers
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	//If not all nodes have registered, reply NotReady
	if ss.numNodes > ss.numRegistered {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
		return nil
	}
	//If OK reply wtih servers list
	reply.Status = storagerpc.OK
	reply.Servers = ss.servers
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
