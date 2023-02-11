package server

import (
	"ceph/monitor/components/mon/server/rpc_proxy"
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// Server mon节点rpc通信模块
type Server struct {
	mu          sync.Mutex             // 资源读写锁
	server      *rpc.Server            // rpc服务端
	listener    net.Listener           // tcp连接监听器
	wg          sync.WaitGroup         // wg用于优雅关闭开启的所有协程
	peerClients map[string]*rpc.Client // 与其他mon节点的rpc客户端
	quitSignal  chan struct{}          // shutdown信号通道

}

func NewServer() *Server {
	return &Server{
		peerClients: make(map[string]*rpc.Client),
		quitSignal:  make(chan struct{}),
	}
}

// Server 挂载rpc代理，监听端口处理rpc请求
func (s *Server) Server() {
	// 注册rpc代理到rpc服务器
	var err error
	s.server.RegisterName("PeersModule", rpc_proxy.PeersRPCProxy{})
	s.server.RegisterName("OthersModule", rpc_proxy.OthersRPCProxy{})
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
	// 关闭listener，防止请求再打进来
	err := s.listener.Close()
	if err != nil {
		log.Fatal(err)
	}
	// quitSignal用于杀掉持续监听tcp接入的go routine
	close(s.quitSignal)
	// 等待所有协程都执行完毕
	s.wg.Wait()
}

// ConnectToPeer 和其他mon节点建立rpc连接，即获取rpc client
// peerId 另一个mon节点id
// addr 另一个mon节点地址
func (s *Server) ConnectToPeer(peerId string, addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		log.Println("peer exists")
		return
	}

	// 如果建立连接失败，那么直接退出程序
	client, err := rpc.Dial(addr.Network(), addr.String())
	if err != nil {
		log.Fatal(err)
	}

	s.peerClients[peerId] = client
}

// DisconnectPeer 断开与指定mon节点的rpc连接
// peerId 另一个mon节点id
func (s *Server) DisconnectPeer(peerId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		log.Println("peer !exists")
		return
	}
	s.peerClients[peerId].Close()
	s.peerClients[peerId] = nil
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for peerId := range s.peerClients {
		if s.peerClients[peerId] == nil {
			continue
		}
		s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
	}
}

// Call 调用指定客户端的指定方法
// peerId 另一个mon节点id
// method rpc方法
// args rpc参数
// reply rpc返回值
func (s *Server) Call(peerId string, method string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	var err error
	client := s.peerClients[peerId]
	if client == nil {
		err = errors.New("peer !exists")
		log.Println(err)
	}
	s.mu.Unlock()

	return client.Call(method, args, reply)
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.listener.Addr()
}
