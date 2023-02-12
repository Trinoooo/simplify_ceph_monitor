package monitor

import (
	"ceph/monitor/detector"
	"ceph/monitor/mon/consensus"
	"ceph/monitor/mon/server"
	"net"
	"sync"
)

type Monitor struct {
	id         string                     // monitor标识
	server     *server.Server             // server模块，负责与其他节点通信
	consensus  *consensus.Consensus       // 共识模块，负责与其他mon保持状态一致
	commitChan chan consensus.CommitEntry // 已提交的命令通知通道
	mu         sync.Mutex
	clusterMap map[string]interface{} // 集群存储拓扑图
	detector   *detector.Detector     // 服务发现模块，所有节点共用
}

func NewMonitor(id string, detector *detector.Detector) *Monitor {
	m := new(Monitor)
	m.id = id
	m.server = server.NewServer(m.consensus, m)
	m.commitChan = make(chan consensus.CommitEntry)
	m.consensus = consensus.NewConsensus(id, m.server, m.commitChan)
	m.clusterMap = make(map[string]interface{})
	m.detector = detector
	return m
}

func (m *Monitor) GetClusterMap() map[string]interface{} {
	return m.clusterMap
}

func (m *Monitor) ReportConsensus() (string, consensus.State, int) {
	return m.consensus.Report()
}

func (m *Monitor) Shutdown() {
	// 终止共识模块
	m.consensus.Stop()
	// 终止server模块
	m.server.Shutdown()
}

func (m *Monitor) GetDetector() *detector.Detector {
	return m.detector
}

func (m *Monitor) GetListenAddr() net.Addr {
	return m.server.GetListenAddr()
}
