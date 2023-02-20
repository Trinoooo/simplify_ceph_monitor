package cephadm

import (
	"log"
	"net"
	"sync"
)

// Cephadm
// 存在Cephadm的意义在于单独维护monitor节点信息
// 这是因为monitor节点地址信息比较特殊
type Cephadm struct {
	mu       sync.Mutex
	monitors map[string]net.Addr
}

func NewCephadm() *Cephadm {
	return &Cephadm{
		monitors: make(map[string]net.Addr),
	}
}

// GetMonitorIds
// 供监控节点调用
func (c *Cephadm) GetMonitorIds() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	res := make([]string, 0)
	for key := range c.monitors {
		res = append(res, key)
	}
	return res
}

// AddMonitor
// 添加监控节点
// 监控节点内部自维护，不需要调用monitor leader节点的register方法
func (c *Cephadm) AddMonitor(id string, addr net.Addr) {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("mon(id:%s) register to cephadm", id)
	c.monitors[id] = addr
}

// RemoveMonitor
// 移除记录的monitor信息
// 可能的情况包括节点主动退出 & leader发现follower不可达
func (c *Cephadm) RemoveMonitor(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("mon(id:%s) remove from cephadm", id)
	delete(c.monitors, id)
}

// GetListenAddr
// 获取monitor节点的地址
func (c *Cephadm) GetListenAddr(id string) net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.monitors[id]
}
