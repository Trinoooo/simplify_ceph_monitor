package rpc_proxy

import (
	monitor "ceph/monitor/mon"
	"ceph/monitor/mon/common"
)

type OthersRPCProxy struct {
	m *monitor.Monitor
}

func NewOthersRPCProxy(m *monitor.Monitor) *OthersRPCProxy {
	return &OthersRPCProxy{
		m: m,
	}
}

type QueryClusterMapArgs struct {
}

type QueryClusterMapReply struct {
	ClusterMap map[string]interface{}
}

// QueryClusterMap
// osd节点查询集群状态图
func (o *OthersRPCProxy) QueryClusterMap(args *QueryClusterMapArgs, reply *QueryClusterMapReply) error {
	reply.ClusterMap = o.m.GetClusterMap()
	return nil
}

type ReportClusterTopologyArgs struct {
	Id       string              // osd节点id
	Topology *common.OSDTopology // osd拓扑链路
}

type ReportClusterTopologyReply struct {
	Success bool
}

// ReportClusterTopology
// osd新加入节点后上报其集群设备拓扑链路图
func (o *OthersRPCProxy) ReportClusterTopology(args *ReportClusterTopologyArgs, reply *ReportClusterTopologyReply) error {

}
