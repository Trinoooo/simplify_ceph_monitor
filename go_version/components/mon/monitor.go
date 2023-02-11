package main

import (
	"ceph/monitor/components/mon/consensus"
	"ceph/monitor/components/mon/server"
)

type Monitor struct {
	id         string                     // monitor标识
	peerIds    []string                   // 与其他mon节点id
	server     *server.Server             // server模块，负责与其他节点通信
	consensus  *consensus.Consensus       // 共识模块，负责与其他mon保持状态一致
	commitChan chan consensus.CommitEntry // 已提交的命令通知通道
}

func NewMonitor(id string, peerIds []string) *Monitor {
	m := new(Monitor)
	m.id = id
	m.peerIds = peerIds
	m.server = server.NewServer()
	m.commitChan = make(chan consensus.CommitEntry)
	m.consensus = consensus.NewConsensus(id, m.server, peerIds, m.commitChan)
	return m
}
