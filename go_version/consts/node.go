package consts

type Liveness int64

const (
	LIVENESS_UP   = 1 // 正常运行
	LIVENESS_DOWN = 2 // 服务停止运行或宕机
)

type Accessibility int64

const (
	ACCESSIBILITY_IN  = 1 // 已接入集群
	ACCESSIBILITY_OUT = 2 // 未接入集群
)

type NodeType int64

const (
	NODE_TYPE_MON = 1
	NODE_TYPE_OSD = 2
	NODE_TYPE_MGR = 3
	NODE_TYPE_MDS = 4
)
