package others

import (
	"ceph/monitor/cephadm"
	"ceph/monitor/consts"
	monitor "ceph/monitor/mon"
	"log"
	"time"
)

type OtherNode struct {
	id       string
	server   *Server
	cephadm  *cephadm.Cephadm // 服务发现组件
	nodeType consts.NodeType  // 节点类型
	quit     chan struct{}
}

func NewOtherNode(id string, cephadm *cephadm.Cephadm, nodeType consts.NodeType, OSDTopology *monitor.OSDTopology) *OtherNode {
	log.Printf("[%s]%s initialization", nodeType, id)
	node := new(OtherNode)
	node.id = id
	node.cephadm = cephadm
	node.nodeType = nodeType
	node.quit = make(chan struct{})
	node.server = NewServer(node, nil)
	// 注册到monitor，之后开启心跳
	go func() {
		// 拿到monitorIds，取第一个id，进行register rpc调用
		monitorIds := node.cephadm.GetMonitorIds()
		if len(monitorIds) <= 0 {
			log.Fatal("boot monitor first")
		}
		args := monitor.RegisterArgs{
			Id:          id,
			Type:        nodeType,
			OSDTopology: OSDTopology,
		}
		reply := monitor.RegisterReply{}
		// 请求出错直接结束
		err := node.server.call(monitorIds[0], "OthersModule.Register", args, &reply)
		if err != nil {
			log.Fatal(err)
		}
		success := false
		leaderId := ""
		for retry := 0; retry < 3; retry++ {
			if reply.Success {
				success = true
				leaderId = reply.LeaderId
				break
			}

			// 如果没有成功注册
			if reply.LeaderId == "" {
				// 如果拿到的是空，说明当前集群没有leader，等一段时间后继续
				time.Sleep(50 * time.Millisecond)
				err = node.server.call(monitorIds[0], "OthersModule.Register", args, &reply)
			} else {
				err = node.server.call(reply.LeaderId, "OthersModule.Register", args, &reply)
			}

			if err != nil {
				log.Fatal(err)
			}
		}
		if !success {
			log.Fatal("register failed")
		}
		log.Printf("[%s]%s register to mon(id:%s)", nodeType, id, leaderId)

		time.Sleep(1 * time.Second)
		// 每100ms发送一次心跳包
		ticker := time.NewTicker(100 * time.Millisecond)
		heartbeatsArgs := monitor.HeartbeatsArgs{
			Id: id,
		}
		for {
			go func() {
				reply := monitor.HeartbeatsReply{}
				err := node.server.call(leaderId, "OthersModule.Heartbeats", heartbeatsArgs, &reply)
				if err != nil {
					log.Println(err)
				}

				// 如果leader更换，则同步更改
				if !reply.Success {
					leaderId = reply.LeaderId
				}
			}()
			<-ticker.C
			select {
			case <-node.quit:
				log.Printf("[%s]%s stop sending RPC requests", node.nodeType, node.id)
				return
			default:
				//
			}
		}
	}()
	return node
}

func (o *OtherNode) Shutdown() {
	log.Printf("[%s]%s shutdown", o.nodeType, o.id)
	close(o.quit)
	o.server.shutdown()
}
