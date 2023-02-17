package rpc_proxy

import (
	monitor "ceph/monitor/mon"
	"ceph/monitor/mon/common"
	"ceph/monitor/mon/consensus"
	"ceph/monitor/others"
)

type OthersRPCProxy struct {
	m *monitor.Monitor
	c *consensus.Consensus
}

func NewOthersRPCProxy(m *monitor.Monitor, c *consensus.Consensus) *OthersRPCProxy {
	return &OthersRPCProxy{
		m: m,
		c: c,
	}
}

type QueryClusterMapArgs struct {
}

type QueryClusterMapReply struct {
	ClusterMap *common.Bucket
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
	Success  bool
	LeaderId string
}

// ReportClusterTopology
// osd新加入节点后上报其集群设备拓扑链路图
func (o *OthersRPCProxy) ReportClusterTopology(args *ReportClusterTopologyArgs, reply *ReportClusterTopologyReply) error {
	return o.m.ReportClusterTopology(args, reply)
}

type RegisterArgs struct {
	Id   string
	Type others.NodeType
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
