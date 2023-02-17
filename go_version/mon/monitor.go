package monitor

import (
	"ceph/monitor/cephadm"
	"ceph/monitor/mon/common"
	"ceph/monitor/mon/consensus"
	"ceph/monitor/mon/server"
	"ceph/monitor/mon/server/rpc_proxy"
	"ceph/monitor/others"
	"errors"
	"fmt"
	set "github.com/deckarep/golang-set"
	jsoniter "github.com/json-iterator/go"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Monitor struct {
	id         string                     // monitor标识
	server     *server.Server             // server模块，负责与其他节点通信
	consensus  *consensus.Consensus       // 共识模块，负责与其他mon保持状态一致
	commitChan chan consensus.CommitEntry // 已提交的命令通知通道
	mu         sync.Mutex                 // 互斥锁，保护线程安全
	clusterMap *common.Bucket             // 集群存储拓扑图
	cephadm    *cephadm.Cephadm           // 服务发现模块，所有节点共用
	// 除monitor节点外其他节点的可达性
	nodes map[string]*common.Node
}

func NewMonitor(id string, cephadm *cephadm.Cephadm) *Monitor {
	m := new(Monitor)
	m.id = id
	m.server = server.NewServer(m.consensus, m.cephadm, map[string]interface{}{
		"PeersModule":  rpc_proxy.NewPeersRPCProxy(m.consensus),
		"OthersModule": rpc_proxy.NewOthersRPCProxy(m, m.consensus),
	})
	m.commitChan = make(chan consensus.CommitEntry)
	m.consensus = consensus.NewConsensus(id, m.server, m.commitChan, m.cephadm, m)
	m.clusterMap = &common.Bucket{
		Children:  make(map[string]interface{}),
		Container: set.NewSet(),
	}
	m.cephadm = cephadm
	m.nodes = make(map[string]*common.Node)

	// 持续监听commitChan发来的提交信息
	go func() {
		// 命令格式 数据对象:操作:ID[:值]
		for entry := range m.commitChan {
			// 将命令字符串拆开
			commands := strings.Split(entry.Command.(string), ":")
			switch commands[0] {
			case "clusterMap":
				switch commands[1] {
				case "ADD":
					// 反序列化出value
					topology := &common.OSDTopology{}
					err := jsoniter.Unmarshal([]byte(commands[3]), topology)
					if err != nil {
						log.Fatal(err)
					}
					m.UpdateClusterMap("ADD", commands[2], topology)
				case "DEL":
					m.UpdateClusterMap("DEL", commands[2], nil)
				default:
					log.Fatal("unreachable")
				}
			case "nodes":
				switch commands[1] {
				case "ADD":
					// 反序列化出value
					var node *common.Node
					err := jsoniter.Unmarshal([]byte(commands[3]), &node)
					if err != nil {
						log.Fatal(err)
					}
					m.UpdateNodes("ADD", commands[2], node)
				case "DEL":
					m.UpdateNodes("DEL", commands[2], nil)
				default:
					log.Fatal("unreachable")
				}
			default:
				log.Fatal("unreachable")
			}
		}
	}()
	return m
}

// GetClusterMap
// 获取监控节点维护的cluster map
func (m *Monitor) GetClusterMap() *common.Bucket {
	return m.clusterMap
}

// Shutdown
// 优雅终止监控节点
func (m *Monitor) Shutdown() {
	// 终止共识模块
	m.consensus.Stop()
	// 终止server模块
	m.server.Shutdown()
}

func (m *Monitor) GetCephadm() *cephadm.Cephadm {
	return m.cephadm
}

func (m *Monitor) GetListenAddr() net.Addr {
	return m.server.GetListenAddr()
}

const CLUSTER_STATE_TEMPLATE = `
	services:
		mon: %d daemons, quorum %s
		mgr: %s(%s)
		osd: %d osds: %d up, %d in
`

func (m *Monitor) ReportClusterState() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	monitorIds := m.cephadm.GetMonitorIds()
	osd, mgr := make([]string, 0), make([]string, 0)
	for id, node := range m.nodes {
		switch node.NodeType {
		case others.NODE_TYPE_OSD:
			osd = append(osd, id)
		case others.NODE_TYPE_MGR:
			mgr = append(mgr, id)
		}
	}
	mgrState := "inactive"
	if len(mgr) > 0 {
		mgrState = "active"
	}
	return fmt.Sprintf(CLUSTER_STATE_TEMPLATE,
		len(monitorIds), strings.Join(monitorIds, ","),
		strings.Join(mgr, ","), mgrState,
		len(osd), len(osd), len(osd),
	)
}

// UpdateClusterMap
// 当logEntry被允许应用到状态机后，对cluster map作出实际修改
func (m *Monitor) UpdateClusterMap(op string, id string, topology *common.OSDTopology) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clusterMap := m.clusterMap
	switch op {
	case "ADD":
		for idx, pathId := range topology.Path {
			// 这段路径在cluster map中存在，则进入下一层
			if node, exist := clusterMap.Children[pathId]; exist {
				switch n := node.(type) {
				case *common.Bucket:
					// 添加其下osd的id，便于删除时快速查找
					// 空间换时间
					n.Container.Add(id)
					n.Weight += topology.Weight
					clusterMap = n
				case *common.Device:
					// 不可能出现这种情况
					log.Fatal("device node among bucket nodes")
				default:
					log.Fatal("invalid node type")
				}
			} else {
				// cluster_map中没有对应节点id说明要新建路径
				// 根据路径id是否是最后一个决定创建什么类型的节点
				if len(topology.Path) == idx+1 {
					clusterMap.Children[pathId] = &common.Device{
						Id:     id,
						Weight: topology.Weight,
						IP:     topology.IP,
						Port:   topology.Port,
					}
				} else {
					bucket := &common.Bucket{
						Weight:    topology.Weight,
						Children:  make(map[string]interface{}),
						Container: set.NewSet(id),
					}
					clusterMap.Children[pathId] = bucket
					clusterMap = bucket
				}
			}
		}
	case "DEL":
		for clusterMap.Container.Contains(id) {
			// 子节点中存在id则移除
			clusterMap.Container.Remove(id)
			for path, node := range clusterMap.Children {
				switch n := node.(type) {
				case *common.Bucket:
					// 子节点包含id则进入下一层
					if n.Container.Contains(id) {
						clusterMap = n
					}
				case *common.Device:
					if n.Id == id {
						// id匹配得上 && 到达叶子节点直接移除
						delete(clusterMap.Children, path)
					}
				default:
					log.Fatal("unreachable")
				}
			}
		}
	default:
		log.Fatal("unreachable")
	}
}

func (m *Monitor) GetNodeState() map[string]time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := make(map[string]time.Time)
	for k, v := range m.nodes {
		res[k] = v.LeastHeartbeat
	}
	return res
}

// UpdateNodes
// 当logEntry被允许应用到状态机后，对nodes数据结构作出实际修改
func (m *Monitor) UpdateNodes(op string, id string, node *common.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch op {
	case "ADD":
		m.nodes[id] = node
	case "DEL":
		delete(m.nodes, id)
	default:
		log.Fatal("unreachable")
	}
}

// ReportClusterTopology
// 外部osd上报其物理存储设备拓扑链路
// 供rpc代理使用
func (m *Monitor) ReportClusterTopology(args *rpc_proxy.ReportClusterTopologyArgs, reply *rpc_proxy.ReportClusterTopologyReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, state, _, leaderId := m.consensus.Report()
	if state == consensus.Leader {
		bytes, err := jsoniter.Marshal(args.Topology)
		if err != nil {
			log.Fatal(err)
		}
		command := fmt.Sprintf(consensus.COMMAND_VALUE_FORMAT, "ADD", "clusterMap", args.Id, string(bytes))
		m.consensus.Submit(command)
		if err != nil {
			log.Println(err)
		}
		reply.Success = true
		return err
	} else {
		reply.Success = false
		reply.LeaderId = leaderId
		return nil
	}
}

// Register
// 外部节点初始化注册到mon leader节点
// 供rpc代理调用
func (m *Monitor) Register(args *rpc_proxy.RegisterArgs, reply *rpc_proxy.RegisterReply) error {
	id, state, _, leaderId := m.consensus.Report()
	if state == consensus.Leader {
		// 先判断
		m.mu.Lock()
		if _, exist := m.nodes[args.Id]; exist {
			reply.Success = false
			return errors.New("node has been registered")
		}
		m.mu.Unlock()
		reply.Success = true
		reply.LeaderId = id
		node := &common.Node{
			LeastHeartbeat: time.Now(),
			NodeType:       args.Type,
		}
		bytes, err := jsoniter.Marshal(node)
		if err != nil {
			log.Fatal(err)
		}
		command := fmt.Sprintf(consensus.COMMAND_VALUE_FORMAT, "ADD", "nodes", args.Id, string(bytes))
		m.consensus.Submit(command)
	} else {
		reply.Success = false
		reply.LeaderId = leaderId
	}
	return nil
}

// Heartbeats
// 外部节点发送心跳包
// 供rpc代理调用
func (m *Monitor) Heartbeats(args *rpc_proxy.HeartbeatsArgs, reply *rpc_proxy.HeartbeatsReply) error {
	_, state, _, leaderId := m.consensus.Report()
	// 如果是leader则处理，否则返回当前leader信息
	if state == consensus.Leader {
		var nodeType others.NodeType
		// 首先判断是否被注册过
		// 没被注册发来心跳直接报错
		// 一种可能的case是心跳包延迟
		m.mu.Lock()
		if _, exist := m.nodes[args.Id]; !exist {
			reply.Success = false
			return errors.New("node not register yet!")
		} else {
			nodeType = m.nodes[args.Id].NodeType
		}
		m.mu.Unlock()

		reply.Success = true
		node := &common.Node{
			LeastHeartbeat: time.Now(),
			NodeType:       nodeType,
		}
		bytes, err := jsoniter.Marshal(node)
		if err != nil {
			log.Fatal(err)
		}
		command := fmt.Sprintf(consensus.COMMAND_VALUE_FORMAT, "ADD", "nodes", args.Id, string(bytes))
		m.consensus.Submit(command)
	} else {
		reply.Success = false
		reply.LeaderId = leaderId
	}
	return nil
}
