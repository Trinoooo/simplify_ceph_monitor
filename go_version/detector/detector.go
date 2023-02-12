package detector

import (
	monitor "ceph/monitor/mon"
	"log"
	"net"
	"sync"
)

type Detector struct {
	mu         sync.Mutex
	nodes      map[string]interface{}
	monitorIds []string
	otherIds   []string // 其他节点存活情况
}

func NewDetector() *Detector {
	detector := new(Detector)
	detector.nodes = make(map[string]interface{})
	detector.monitorIds = make([]string, 0)
	detector.otherIds = make([]string, 0)
	return detector
}

// Add 向分布式系统中添加节点
func (d *Detector) Add(id string, node interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 判断是否存在，存在则不添加
	if _, exist := d.nodes[id]; exist {
		log.Println("node exists")
		return
	}

	// 保护性编程
	if _, ok := node.(*monitor.Monitor); ok {
		d.monitorIds = append(d.monitorIds, id)
	}
	d.nodes[id] = node
}

type Shutdowner interface {
	Shutdown()
}

// Remove 移除分布式系统中的节点
func (d *Detector) Remove(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 判断是否存在，不存在直接返回
	node, exist := d.nodes[id]
	if !exist {
		log.Println("node !exists")
		return
	}

	// 清除map和slice
	// 先通过shutdown关闭连接
	node.(Shutdowner).Shutdown()

	// 再清理detector内的数据结构
	delete(d.nodes, id)
	if _, ok := node.(*monitor.Monitor); ok {
		for idx, monId := range d.monitorIds {
			if monId == id {
				d.monitorIds = append(d.monitorIds[:idx], d.monitorIds[idx+1:]...)
				return
			}
		}
		log.Fatal("unreachable")
	}
}

func (d *Detector) GetMonitorIds() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	res := make([]string, len(d.monitorIds))
	copy(res, d.monitorIds)
	return res
}

type ListenAddr interface {
	GetListenAddr() net.Addr
}

func (d *Detector) GetListenAddr(id string) net.Addr {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.nodes[id].(ListenAddr).GetListenAddr()
}

func (d *Detector) GetNode(id string) interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	node, exists := d.nodes[id]
	if !exists {
		log.Fatal("unreachable")
	}
	return node
}

func (d *Detector) GetOtherIds() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	res := make([]string, 0)
	copy(res, d.otherIds)
	return res
}
