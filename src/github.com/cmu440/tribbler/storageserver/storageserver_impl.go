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
	numNodes       int
	servers        []storagerpc.Node
	numRegistered  int
	newServer      chan storagerpc.Node
	newServerReply chan storagerpc.RegisterReply
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
		numNodes:      numNodes,
		servers:       make([]storagerpc.Node, numNodes),
		numRegistered: 1,
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
