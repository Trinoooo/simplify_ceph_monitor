package monitor

import (
	"ceph/monitor/consts"
	"time"
)

// OSDTopology osd新节点携带的物理拓扑图信息
type OSDTopology struct {
	Weight int64    `json:"weight"`
	Path   []string `json:"path"`
	IP     string   `json:"ip"`
	Port   uint16   `json:"port"`
}

// Node 维护外部节点信息
type Node struct {
	LeastHeartbeat time.Time
	NodeType       consts.NodeType
}
