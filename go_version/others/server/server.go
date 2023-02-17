package server

import (
	"ceph/monitor/others"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	mu      sync.Mutex
	server  *rpc.Server
	wg      sync.WaitGroup
	clients map[string]*rpc.Client
	node    *others.OtherNode
}

func NewServer(node *others.OtherNode, services map[string]interface{}) *Server {
	server := new(Server)
	server.server = rpc.NewServer()
	server.clients = make(map[string]*rpc.Client)
	server.node = node
	return server
}

// ConnectToNode 和其他节点建立rpc连接，即获取rpc client
// id 另一个节点id
// addr 另一个节点地址
func (s *Server) connectToNode(id string, addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients[id] != nil {
		log.Println("node exists")
		return
	}

	// 如果建立连接失败，那么直接退出程序
	client, err := rpc.Dial(addr.Network(), addr.String())
	if err != nil {
		log.Fatal(err)
	}

	s.clients[id] = client
}

func (s *Server) disconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.clients {
		if s.clients[id] == nil {
			continue
		}
		s.clients[id].Close()
		s.clients[id] = nil
	}
}

// Shutdown
// 停止当前节点，中断其中所有连接
func (s *Server) Shutdown() {
	s.disconnectAll()
	s.wg.Wait()
}

func (s *Server) Call(id, method string, args, reply interface{}) error {
	s.mu.Lock()
	// 获取客户端，如果没有则建立连接
	client := s.clients[id]
	s.mu.Unlock()

	if client == nil {
		addr := s.node.GetCephadm().GetListenAddr(id)
		s.connectToNode(id, addr)
	}

	return client.Call(method, args, reply)
}
