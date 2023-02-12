package others

import (
	"ceph/monitor/detector"
	"ceph/monitor/others/server"
)

type OtherNode struct {
	id       string
	server   *server.Server
	detector *detector.Detector // 服务发现组件
}

func NewOtherNode(id string, detector *detector.Detector) *OtherNode {
	node := new(OtherNode)
	node.id = id
	node.server = server.NewServer(node)
	return node
}

func (o *OtherNode) GetDetector() *detector.Detector {
	return o.detector
}
