package server

import (
	"ceph/monitor/others"
	"ceph/monitor/others/server/rpc_proxy"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	mu         sync.Mutex
	server     *rpc.Server
	listener   net.Listener
	wg         sync.WaitGroup
	quitSignal chan struct{}
	clients    map[string]*rpc.Client
	node       *others.OtherNode
}

func NewServer(node *others.OtherNode) *Server {
	server := new(Server)
	server.server = rpc.NewServer()
	server.quitSignal = make(chan struct{})
	server.clients = make(map[string]*rpc.Client)
	server.node = node
	server.Serve()
	return server
}

func (s *Server) Serve() {
	// 注册rpc服务
	s.server.RegisterName("HeartbeatsModule", rpc_proxy.Heartbeats{})
	var err error

	// 开始监听本地随机端口
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quitSignal:
					return
				default:
					log.Fatal(err)
				}
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.server.ServeConn(conn)
			}()
		}
	}()
}

// ConnectToNode 和其他节点建立rpc连接，即获取rpc client
// id 另一个节点id
// addr 另一个节点地址
func (s *Server) ConnectToNode(id string, addr net.Addr) {
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

// DisconnectNode 断开与指定节点的rpc连接
// id 另一个节点id
func (s *Server) DisconnectNode(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients[id] == nil {
		log.Println("node !exists")
		return
	}
	s.clients[id].Close()
	s.clients[id] = nil
}

func (s *Server) DisconnectAll() {
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
	close(s.quitSignal)
	s.listener.Close()
	s.wg.Wait()
	s.DisconnectAll()
}

func (s *Server) GetListenAddr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Call(id, method string, args, reply interface{}) error {
	s.mu.Lock()
	// 获取客户端，如果没有则建立连接
	client := s.clients[id]
	s.mu.Unlock()

	if client == nil {
		addr := s.node.GetDetector().GetListenAddr(id)
		s.ConnectToNode(id, addr)
	}

	return client.Call(method, args, reply)
}
