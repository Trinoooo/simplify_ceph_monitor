package consts

import "log"

type NodeType int

const (
	NODE_TYPE_OSD = 1
	NODE_TYPE_MGR = 2
)

func (t NodeType) String() string {
	switch t {
	case NODE_TYPE_MGR:
		return "MGR"
	case NODE_TYPE_OSD:
		return "OSD"
	default:
		log.Fatal("unreachable")
	}
	return ""
}
