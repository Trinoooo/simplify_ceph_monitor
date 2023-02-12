package common

import "ceph/monitor/consts"

// OSDTopology osd新节点携带的物理拓扑图信息
type OSDTopology struct {
	Weight int64    `json:"weight"`
	Path   []string `json:"path"`
	IP     string   `json:"ip"`
	Port   uint16   `json:"port"`
}

// Node ceph分布式系统中各功能节点状态
type Node struct {
	Liveness      consts.Liveness      `json:"liveness"`      // 节点活性
	Accessibility consts.Accessibility `json:"accessibility"` // 节点可访问性
}
