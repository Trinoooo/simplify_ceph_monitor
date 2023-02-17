package others

import (
	"ceph/monitor/cephadm"
	"ceph/monitor/mon/server/rpc_proxy"
	"ceph/monitor/others/server"
	"log"
	"sync"
	"time"
)

type NodeType int

const (
	NODE_TYPE_OSD = 1
	NODE_TYPE_MGR = 2
)

type OtherNode struct {
	id       string
	server   *server.Server
	cephadm  *cephadm.Cephadm // 服务发现组件
	nodeType NodeType         // 节点类型
	wg       sync.WaitGroup
}

func NewOtherNode(id string, cephadm *cephadm.Cephadm, nodeType NodeType) *OtherNode {
	node := new(OtherNode)
	node.id = id
	node.cephadm = cephadm
	node.nodeType = nodeType
	// 注册到monitor，之后开启心跳
	node.wg.Add(1)
	go func() {
		defer node.wg.Done()
		// 拿到monitorIds，取第一个id，进行register rpc调用
		monitorIds := node.cephadm.GetMonitorIds()
		args := rpc_proxy.RegisterArgs{
			Id: id,
		}
		reply := rpc_proxy.RegisterReply{}
		// 请求出错直接结束
		err := node.server.Call(monitorIds[0], "OthersModule.Register", args, &reply)
		if err != nil {
			log.Fatal(err)
		}
		success := false
		leaderId := ""
		for i := 0; i < 3; i++ {
			if reply.Success {
				success = true
				leaderId = reply.LeaderId
				break
			}

			// 如果没有成功注册
			if reply.LeaderId == "" {
				// 如果拿到的是空，说明当前集群没有leader，等一段时间后继续
				time.Sleep(50 * time.Millisecond)
				err = node.server.Call(monitorIds[0], "OthersModule.Register", args, &reply)
				continue
			} else {
				err = node.server.Call(reply.LeaderId, "OthersModule.Register", args, &reply)
			}

			if err != nil {
				log.Fatal(err)
			}
		}
		if !success {
			log.Fatal("register failed")
		}

		ticker := time.NewTicker(30 * time.Millisecond)
		heartbeatsArgs := rpc_proxy.HeartbeatsArgs{
			Id: id,
		}
		for {
			node.wg.Add(1)
			go func() {
				defer node.wg.Done()
				reply := rpc_proxy.HeartbeatsReply{}
				err := node.server.Call(leaderId, "OthersModule.Heartbeats", heartbeatsArgs, &reply)
				if err != nil {
					log.Println(err)
				}

				// 如果leader更换，则同步更改
				if !reply.Success {
					leaderId = reply.LeaderId
				}
			}()
			<-ticker.C
		}
	}()
	return node
}

func (o *OtherNode) GetCephadm() *cephadm.Cephadm {
	return o.cephadm
}

func (o *OtherNode) GetType() NodeType {
	return o.nodeType
}

func (o *OtherNode) GetId() string {
	return o.id
}
