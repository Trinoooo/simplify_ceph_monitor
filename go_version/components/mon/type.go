//package main
//
//import (
//	"ceph/monitor/components/common"
//	"ceph/monitor/consts"
//	"encoding/json"
//	"errors"
//	"io"
//	"log"
//	"sync"
//)
//
//// Monitor 监视器
//type aa struct {
//	MONStateMap map[string]*common.Node         // mon集群状态表
//	OSDStateMap map[string]*common.Node         // osd集群状态表
//	MGRStateMap map[string]*common.Node         // mgr集群状态表
//	MDSStateMap map[string]*common.Node         // mds集群状态表
//	ClusterMap  map[string]interface{}          // osd集群设备拓扑图
//	Role        consts.RaftRole                 // 集群中的角色
//	Locks       map[consts.NodeType]*sync.Mutex // map读写锁
//}
//
//func NewMonitor() *Monitor {
//	return &Monitor{
//		MONStateMap: make(map[string]*common.Node),
//		OSDStateMap: make(map[string]*common.Node),
//		MGRStateMap: make(map[string]*common.Node),
//		MDSStateMap: make(map[string]*common.Node),
//		ClusterMap:  make(map[string]interface{}),
//		Role:        consts.RAFT_ROLE_CANIDATE,
//		Locks: map[consts.NodeType]*sync.Mutex{
//			consts.NODE_TYPE_OSD: {},
//			consts.NODE_TYPE_MON: {},
//			consts.NODE_TYPE_MGR: {},
//			consts.NODE_TYPE_MDS: {},
//		},
//	}
//}
//
//// DumpClusterMap 将监控节点维护的集群拓扑图序列化，用于其他节点查询时作为结果返回
//func (m *Monitor) DumpClusterMap() ([]byte, error) {
//	marshal, err := json.Marshal(m.ClusterMap)
//	log.Printf("clustermap after dump: %s", string(marshal))
//	return marshal, err
//}
//
//// OutputCephClusterState 输出ceph集群状态到指定目的地
//// w 自定义输出目的地
//func (m *Monitor) OutputCephClusterState(w io.Writer) error {
//
//}
//
//// Register 注册节点到mon
//func (m *Monitor) Register(nodeId string, nodeType consts.NodeType, nodeInfo *common.Node, topology *common.OSDTopology) error {
//	switch nodeType {
//	case consts.NODE_TYPE_OSD:
//		if topology == nil {
//			log.Printf("topology should not be nil when register node type is OSD")
//			return errors.New("invalid register request")
//		}
//		return handleOSDNode(nodeId, nodeInfo, topology, m.Locks[consts.NODE_TYPE_OSD], m.OSDStateMap, m.ClusterMap)
//	case consts.NODE_TYPE_MON:
//		return handleMONNode(nodeId, nodeInfo, m.Locks[consts.NODE_TYPE_MON], m.MONStateMap)
//	case consts.NODE_TYPE_MGR:
//		return handleMGRNode(nodeId, nodeInfo, m.Locks[consts.NODE_TYPE_MGR], m.MGRStateMap)
//	case consts.NODE_TYPE_MDS:
//		return handleMDSNode(nodeId, nodeInfo, m.Locks[consts.NODE_TYPE_MDS], m.MDSStateMap)
//	default:
//		return errors.New("invalid node type")
//	}
//}
//
//func handleOSDNode(id string, info *common.Node, topology *common.OSDTopology, lock *sync.Mutex, m map[string]*common.Node, cm map[string]interface{}) error {
//	// 要分别修改osd_state_map和cluster_map，需要加锁保证线程安全
//	lock.Lock()
//	defer lock.Unlock()
//
//	// STEP1：向osd_state_map添加osd信息
//	if _, exist := m[id]; exist {
//		log.Printf("key has already exist")
//		return errors.New("key key has already exist")
//	}
//	m[id] = info
//
//	// STEP2：更新cluster_map
//	err := updateClusterMap(cm, topology)
//	if err != nil {
//		log.Printf("call updateClusterMap failed, err = %#v", err)
//		return err
//	}
//
//	return nil
//}
//
//func updateClusterMap(clusterMap map[string]interface{}, topology *common.OSDTopology) error {
//	for idx, pathId := range topology.Path {
//		if node, exist := clusterMap[pathId]; exist {
//			switch n := node.(type) {
//			case common.Bucket:
//				n.Weight += topology.Weight
//				clusterMap = n.Children
//			case common.Device:
//				// 不可能出现这种情况
//				log.Printf("device node among bucket nodes")
//				return errors.New("device node among bucket nodes")
//			default:
//				log.Printf("invalid node type")
//				return errors.New("invalid node type")
//			}
//		} else { // cluster_map中没有对应节点id说明要新建路径
//			// 根据路径id是否是最后一个决定创建什么类型的节点
//			if len(topology.Path) == idx+1 {
//				clusterMap[pathId] = &common.Device{
//					Weight: topology.Weight,
//					IP:     topology.IP,
//					Port:   topology.Port,
//				}
//			} else {
//				childMap := make(map[string]interface{})
//				clusterMap[pathId] = &common.Bucket{
//					Weight:   topology.Weight,
//					Children: childMap,
//				}
//				clusterMap = childMap
//			}
//		}
//	}
//
//	return nil
//}
//
//func handleMONNode(id string, info *common.Node, lock *sync.Mutex, m map[string]*common.Node) error {
//	// 给map加锁，保证线程安全
//	lock.Lock()
//	defer lock.Unlock()
//
//	// 避免同一个节点重复添加
//	if _, exist := m[id]; exist {
//		log.Printf("key has already exist")
//		return errors.New("key key has already exist")
//	}
//	m[id] = info
//	return nil
//}
//
//func handleMGRNode(id string, info *common.Node, lock *sync.Mutex, m map[string]*common.Node) error {
//	// 给map加锁，保证线程安全
//	lock.Lock()
//	defer lock.Unlock()
//
//	// 避免同一个节点重复添加
//	if _, exist := m[id]; exist {
//		log.Printf("key has already exist")
//		return errors.New("key key has already exist")
//	}
//	m[id] = info
//	return nil
//}
//
//func handleMDSNode(id string, info *common.Node, lock *sync.Mutex, m map[string]*common.Node) error {
//	// 给map加锁，保证线程安全
//	lock.Lock()
//	defer lock.Unlock()
//
//	// 避免同一个节点重复添加
//	if _, exist := m[id]; exist {
//		log.Printf("key has already exist")
//		return errors.New("key key has already exist")
//	}
//	m[id] = info
//	return nil
//}
