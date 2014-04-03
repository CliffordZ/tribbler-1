package storageserver

import (
	"errors"
  "net"
	"github.com/cmu440/tribbler/rpc/storagerpc"
        "net/http"
        "net/rpc"
        "strconv"
        "time"
)

type storageServer struct {
  listener *net.Listener
  masterServerHostPort string
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
  listener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
  if err != nil {
    return nil, err
  }

  storageServer := &storageServer{
    listener : &listener,
    masterServerHostPort : masterServerHostPort,
  }

  err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer))
  if err != nil {
    return nil, err
  }
  
  rpc.HandleHTTP()
  go http.Serve(listener, nil)

  node := &storagerpc.Node{
    HostPort : "localhost:"+strconv.Itoa(port),
    NodeID : nodeID,
  }

  //If not master server, wait for slave servers to register
  if masterServerHostPort != "" {
    //TODO: make calls to register to get slave's rpc
    args := &storagerpc.RegisterArgs{
      ServerInfo : *node,
    }
    reply := &storagerpc.RegisterReply{} 
    for true {
      if err = StorageServer.Call("StorageServer.RegisterServer", args, reply); err != nil {
        return nil, err
      }
      if reply.Status == storagerpc.NotReady {
        time.Sleep(time.Second)
      } else {
        break
      }
    }
  }


  return storageServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
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
