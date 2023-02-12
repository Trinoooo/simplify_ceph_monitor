package rpc_proxy

import (
	"ceph/monitor/mon/consensus"
)

// PeersRPCProxy mon集群中对等体间调用的rpc方法
// 只涉及分布式共识（心跳包含在内）
type PeersRPCProxy struct {
	c *consensus.Consensus
}

func NewPeersRPCProxy(c *consensus.Consensus) *PeersRPCProxy {
	return &PeersRPCProxy{
		c: c,
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote 处理候选者发送来的投票请求
func (p *PeersRPCProxy) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return p.c.RequestVote(args, reply)
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId string

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*consensus.LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries 心跳包 & 追加日志
func (p *PeersRPCProxy) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return p.c.AppendEntries(args, reply)
}
