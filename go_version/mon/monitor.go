package monitor

import (
	"ceph/monitor/cephadm"
	"ceph/monitor/consts"
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
	id         string           // monitor标识
	server     *Server          // server模块，负责与其他节点通信
	consensus  *Consensus       // 共识模块，负责与其他mon保持状态一致
	commitChan chan CommitEntry // 已提交的命令通知通道
	mu         sync.Mutex       // 互斥锁，保护线程安全
	clusterMap *Bucket          // 集群存储拓扑图
	cephadm    *cephadm.Cephadm // 服务发现模块，所有节点共用
	// 除monitor节点外其他节点的可达性
	nodes map[string]*Node
	ready chan struct{} // 就绪信号
}

func NewMonitor(id string, cephadm *cephadm.Cephadm) *Monitor {
	log.Printf("mon(id:%s) start initialization", id)
	m := new(Monitor)
	m.id = id
	m.commitChan = make(chan CommitEntry)
	m.ready = make(chan struct{})
	m.cephadm = cephadm
	m.consensus = NewConsensus(id, m.commitChan, m.cephadm, m, m.ready)
	m.server = NewServer(m.id, m.cephadm, map[string]interface{}{
		"PeersModule":  NewPeersRPCProxy(m.consensus),
		"OthersModule": NewOthersRPCProxy(m, m.consensus),
	})
	m.clusterMap = &Bucket{
		Children:  make(map[string]interface{}),
		Container: set.NewSet(),
	}
	m.nodes = make(map[string]*Node)
	// 持续监听commitChan发来的提交信息
	go func() {
		log.Printf("mon(id:%s) start listen to commitChan", id)
		// 命令格式 数据对象:操作:ID[:值]
		for entry := range m.commitChan {
			log.Printf("mon(id:%s) logEntry arrived，command：%v", id, entry.Command)
			// 将命令字符串拆开
			commands := strings.Split(entry.Command.(string), "::")
			log.Printf("mon(id:%s) content of command: %#v", m.id, commands)
			switch commands[1] {
			case "clusterMap":
				switch commands[0] {
				case "ADD":
					// 反序列化出value
					topology := &OSDTopology{}
					err := jsoniter.Unmarshal([]byte(commands[3]), topology)
					if err != nil {
						log.Fatal(err)
					}
					m.updateClusterMap("ADD", commands[2], topology)
				case "DEL":
					m.updateClusterMap("DEL", commands[2], nil)
				default:
					log.Fatal("unreachable")
				}
			case "nodes":
				switch commands[0] {
				case "ADD":
					// 反序列化出value
					var node *Node
					err := jsoniter.Unmarshal([]byte(commands[3]), &node)
					if err != nil {
						log.Fatal(err)
					}
					m.updateNodes("ADD", commands[2], node)
				case "DEL":
					m.updateNodes("DEL", commands[2], nil)
				default:
					log.Fatal("unreachable")
				}
			default:
				log.Fatal("unreachable")
			}
		}
	}()

	// 初始化完成后注册到cephadm
	defer m.cephadm.AddMonitor(id, m.server.getListenAddr())
	return m
}

// GetClusterMap
// 获取监控节点维护的cluster map
func (m *Monitor) GetClusterMap() *Bucket {
	return m.clusterMap
}

// Shutdown
// 优雅终止监控节点
func (m *Monitor) Shutdown() {
	log.Printf("mon(id:%s) shutdown", m.id)
	// 从cephadm注销
	m.cephadm.RemoveMonitor(m.id)
	// 终止共识模块
	m.consensus.stop()
	// 终止server模块
	m.server.shutdown()
}

func (m *Monitor) GetListenAddr() net.Addr {
	return m.server.getListenAddr()
}

const CLUSTER_STATE_TEMPLATE = `
	services:
		mon: %d daemons, quorum %s
		mgr: %s(%s)
		osd: %d osds
`

func (m *Monitor) ReportClusterState() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	monitorIds := m.cephadm.GetMonitorIds()
	osd, mgr := make([]string, 0), make([]string, 0)
	for id, node := range m.nodes {
		switch node.NodeType {
		case consts.NODE_TYPE_OSD:
			osd = append(osd, id)
		case consts.NODE_TYPE_MGR:
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
		len(osd),
	)
}

// updateClusterMap
// 当logEntry被允许应用到状态机后，对cluster map作出实际修改
func (m *Monitor) updateClusterMap(op string, id string, topology *OSDTopology) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clusterMap := m.clusterMap
	switch op {
	case "ADD":
		for idx, pathId := range topology.Path {
			// 这段路径在cluster map中存在，则进入下一层
			if node, exist := clusterMap.Children[pathId]; exist {
				switch n := node.(type) {
				case *Bucket:
					// 添加其下osd的id，便于删除时快速查找
					// 空间换时间
					n.Container.Add(id)
					n.Weight += topology.Weight
					clusterMap = n
				case *Device:
					// 不可能出现这种情况
					log.Fatal("device node among bucket nodes")
				default:
					log.Fatal("invalid node type")
				}
			} else {
				// cluster_map中没有对应节点id说明要新建路径
				// 根据路径id是否是最后一个决定创建什么类型的节点
				if len(topology.Path) == idx+1 {
					clusterMap.Children[pathId] = &Device{
						Id:     id,
						Weight: topology.Weight,
						IP:     topology.IP,
						Port:   topology.Port,
					}
				} else {
					bucket := &Bucket{
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
				case *Bucket:
					// 子节点包含id则进入下一层
					if n.Container.Contains(id) {
						clusterMap = n
					}
				case *Device:
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

	// 输出最新cluster map
	bytes, err := jsoniter.Marshal(m.clusterMap)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("mon(id:%s) cluster map changed, result: %#v", m.id, string(bytes))
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

// updateNodes
// 当logEntry被允许应用到状态机后，对nodes数据结构作出实际修改
func (m *Monitor) updateNodes(op string, id string, node *Node) {
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
func (m *Monitor) ReportClusterTopology(args *ReportClusterTopologyArgs, reply *ReportClusterTopologyReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, state, _, leaderId := m.consensus.report()
	if state == Leader {
		bytes, err := jsoniter.Marshal(args.Topology)
		if err != nil {
			log.Fatal(err)
		}
		command := fmt.Sprintf(COMMAND_VALUE_FORMAT, "ADD", "clusterMap", args.Id, string(bytes))
		m.consensus.submit(command)
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
func (m *Monitor) Register(args *RegisterArgs, reply *RegisterReply) error {
	id, state, _, leaderId := m.consensus.report()
	if state == Leader {
		// 先判断
		m.mu.Lock()
		if _, exist := m.nodes[args.Id]; exist {
			reply.Success = false
			return errors.New("node has been registered")
		}
		m.mu.Unlock()
		reply.Success = true
		reply.LeaderId = id
		node := &Node{
			LeastHeartbeat: time.Now(),
			NodeType:       args.Type,
		}
		bytes, err := jsoniter.Marshal(node)
		if err != nil {
			log.Fatal(err)
		}
		command := fmt.Sprintf(COMMAND_VALUE_FORMAT, "ADD", "nodes", args.Id, string(bytes))
		m.consensus.submit(command)

		// 如果是osd节点加入的话还要额外把物理存储拓扑图更新
		if args.Type == consts.NODE_TYPE_OSD {
			bytes, err = jsoniter.Marshal(args.OSDTopology)
			if err != nil {
				log.Fatal(err)
			}
			command = fmt.Sprintf(COMMAND_VALUE_FORMAT, "ADD", "clusterMap", args.Id, string(bytes))
			m.consensus.submit(command)
		}
	} else {
		reply.Success = false
		reply.LeaderId = leaderId
	}
	return nil
}

// Heartbeats
// 外部节点发送心跳包
// 供rpc代理调用
func (m *Monitor) Heartbeats(args *HeartbeatsArgs, reply *HeartbeatsReply) error {
	log.Printf("mon(id:%s) received heartbeat msg from %s", m.id, args.Id)
	_, state, _, leaderId := m.consensus.report()
	// 如果是leader则处理，否则返回当前leader信息
	if state == Leader {
		var nodeType consts.NodeType
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
		node := &Node{
			LeastHeartbeat: time.Now(),
			NodeType:       nodeType,
		}
		bytes, err := jsoniter.Marshal(node)
		if err != nil {
			log.Fatal(err)
		}
		command := fmt.Sprintf(COMMAND_VALUE_FORMAT, "ADD", "nodes", args.Id, string(bytes))
		m.consensus.submit(command)
	} else {
		reply.Success = false
		reply.LeaderId = leaderId
	}
	return nil
}

// Ready
// 开启共识模块
func (m *Monitor) Ready() {
	m.ready <- struct{}{}
}

func (m *Monitor) ReportConsensusState() {
	log.Println(m.consensus.report())
}

type OthersRPCProxy struct {
	m *Monitor
	c *Consensus
}

func NewOthersRPCProxy(m *Monitor, c *Consensus) *OthersRPCProxy {
	return &OthersRPCProxy{
		m: m,
		c: c,
	}
}

type QueryClusterMapArgs struct {
}

type QueryClusterMapReply struct {
	ClusterMap *Bucket
}

// QueryClusterMap
// osd节点查询集群状态图
func (o *OthersRPCProxy) QueryClusterMap(args *QueryClusterMapArgs, reply *QueryClusterMapReply) error {
	reply.ClusterMap = o.m.GetClusterMap()
	return nil
}

type ReportClusterTopologyArgs struct {
	Id       string       // osd节点id
	Topology *OSDTopology // osd拓扑链路
}

type ReportClusterTopologyReply struct {
	Success  bool
	LeaderId string
}

// ReportClusterTopology
// osd新加入节点后上报其集群设备拓扑链路图
func (o *OthersRPCProxy) ReportClusterTopology(args *ReportClusterTopologyArgs, reply *ReportClusterTopologyReply) error {
	return o.m.ReportClusterTopology(args, reply)
}

type RegisterArgs struct {
	Id          string
	Type        consts.NodeType
	OSDTopology *OSDTopology
}

type RegisterReply struct {
	Success  bool
	LeaderId string
}

// Register
// 其他类型节点注册到monitor
func (o *OthersRPCProxy) Register(args *RegisterArgs, reply *RegisterReply) error {
	return o.m.Register(args, reply)
}

type HeartbeatsArgs struct {
	Id string
}

type HeartbeatsReply struct {
	Success  bool
	LeaderId string
}

// Heartbeats
// 其他节点主动和leader发送心跳包
func (o *OthersRPCProxy) Heartbeats(args *HeartbeatsArgs, reply *HeartbeatsReply) error {
	return o.m.Heartbeats(args, reply)
}
