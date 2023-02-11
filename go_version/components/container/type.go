package main

import (
	"ceph/monitor/components/common"
	"ceph/monitor/utils"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"log"
	"net"
)

// MonitorContainer 盛放mon节点的容器，目的是代理mon主节点，避免其他节点查找哪个是mon主节点
// mon_container内部自维护状态，外部可将mon集群看作不会出现但单点故障的"逻辑单机"
type MonitorContainer struct {
	Monitors map[string]*Monitor // 维护的所有监视器
	Count    int64               // 当前监视器数量
	Master   *monitor.Monitor    // 当前监视器主节点
}

func NewMonitorContainer() *MonitorContainer {
	// double check保证多线程单例线程安全
	if mc == nil {
		lock.Lock()
		defer lock.Unlock()
		if mc == nil {
			mc = &MonitorContainer{
				Monitors: make(map[string]*monitor.Monitor),
				Count:    0,
			}
		}
	}
	return mc
}

func (mc *MonitorContainer) Register(ip, port string) error {
	// 主动连接加入节点，创建tcp连接
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", ip, port))
	if err != nil {
		log.Fatal(err)
	}

	// 从tcp中读取新加入节点源数据
	bytes, err := utils.ReadAll(conn)
	if err != nil {
		log.Printf("call utils.ReadAll failed. err = %#v", err)
		return err
	}

	req := utils.RegisterReq{}
	if err = jsoniter.Unmarshal(bytes, &req); err != nil {
		log.Printf("error occur when json unmarshal, err = %#v", err)
		return err
	}

	if err = mc.Master.Register(req.NodeId, req.NodeType, &common.Node{
		Liveness:      req.Liveness,
		Accessibility: req.Accessibility,
	}, req.OSDTopology); err != nil {
		log.Printf("error occur when register node to mon")
		return err
	}

	return nil
}

func parseRegisterRequest() {

}
