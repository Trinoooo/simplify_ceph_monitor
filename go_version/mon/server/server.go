package server

import (
	"ceph/monitor/cephadm"
	"ceph/monitor/mon/consensus"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// Server mon节点rpc通信模块
type Server struct {
	mu         sync.Mutex             // 资源读写锁
	server     *rpc.Server            // rpc服务端
	listener   net.Listener           // tcp连接监听器
	wg         sync.WaitGroup         // wg用于优雅关闭开启的所有协程
	clients    map[string]*rpc.Client // 与其他节点的rpc客户端
	quitSignal chan struct{}          // shutdown信号通道
	consensus  *consensus.Consensus
	cephadm    *cephadm.Cephadm
}

func NewServer(c *consensus.Consensus, cephadm *cephadm.Cephadm, services map[string]interface{}) *Server {
	server := new(Server)
	server.clients = make(map[string]*rpc.Client)
	server.quitSignal = make(chan struct{})
	server.consensus = c
	server.cephadm = cephadm
	server.Server(services)
	return server
}

// Server 挂载rpc代理，监听端口处理rpc请求
func (s *Server) Server(services map[string]interface{}) {
	// 注册rpc代理到rpc服务器
	var err error
	for name, service := range services {
		s.server.RegisterName(name, service)
	}
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	// 开一个协程持续监听打进来的rpc请求
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

			// 开启一个新协程处理每个请求处理
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.server.ServeConn(conn)
			}()
		}
	}()
}

// Shutdown 优雅终止monitor节点
func (s *Server) Shutdown() {
	// quitSignal用于接收一直阻塞在accept处的go routine
	close(s.quitSignal)
	// 关闭listener，防止请求再打进来
	s.listener.Close()
	// 等待所有协程都执行完毕
	s.wg.Wait()
	s.disconnectAll()
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

// Call 调用指定客户端的指定方法
// id 另一个节点id
// method rpc方法
// args rpc参数
// reply rpc返回值
func (s *Server) Call(id string, method string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	// 获取客户端，如果没有则建立连接
	client := s.clients[id]
	s.mu.Unlock()

	if client == nil {
		addr := s.cephadm.GetListenAddr(id)
		s.connectToNode(id, addr)
	}

	return client.Call(method, args, reply)
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.listener.Addr()
}
