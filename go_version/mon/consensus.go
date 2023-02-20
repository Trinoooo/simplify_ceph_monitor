package monitor

import (
	"ceph/monitor/cephadm"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type CommitEntry struct {
	Command interface{} // 命令内容
	Index   int         // 日志索引
	Term    int         // 日志任期
}

type State int

const (
	Follower  State = 1
	Candidate State = 2
	Leader    State = 3
	Dead      State = 4
)

// 日志中command的格式
// 操作:数据对象:id[:值]
const (
	COMMAND_VALUE_FORMAT = "%s::%s::%s::%s"
	COMMAND_FORMAT       = "%s::%s::%s"
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// Consensus
// 共识的一个重要原则是永远不接受比自己任期小的请求
type Consensus struct {
	id          string           // id
	mu          sync.Mutex       // 互斥锁，保护线程安全
	logs        []*LogEntry      // 日志
	cephadm     *cephadm.Cephadm // admin模块
	monitor     *Monitor         // 监控节点
	state       State            // 节点状态
	currentTerm int              // 当前任期
	votedFor    string           // 刚给谁投过票
	// 选举重置时间，以下操作会使节点的选举重置时间刷新
	// * follower收到心跳
	// * follower转换为candidate
	// * follower或candidate选举计时器超时
	electionResetTime time.Time
	// 命令提交到外部模块的通道
	// 共识模块只提供leader选举和命令同步服务
	// 至于命令的执行需要节点状态机进行应用
	commitChan chan CommitEntry
	// 多数节点保存日志后的应用日志通知通道，通知给初始化时启动的监听协程
	newCommitReadyChan chan struct{}
	//
	// ｜		｜		｜		｜		｜		｜		｜		｜
	// ｜	0	｜	1	｜	2	｜	3	｜	4	｜	5	｜	6	｜
	// ｜		｜		｜		｜		｜		｜		｜		｜
	// 						^				^
	// 						lastApplied		|
	// 										commitIndex
	// lastApplied 状态机已经应用的最后一个日志索引，该索引前面的所有日志实体都已经被当前节点的状态机应用
	// commitIndex 状态机已经允许应用的最后一个日志索引，即[lastApplied + 1, commitIndex + 1]都可以被状态机应用
	lastApplied int
	commitIndex int
	nextIndex   map[string]int // 集群中其他节点的下一个带被接受的logEntry索引
	matchIndex  map[string]int // 集群中其他节点的已经被存储的最后一个logEntry的索引
	leaderId    string         // 当前集群中monitor leader节点的id
	ready       chan struct{}
}

func NewConsensus(id string, commitChan chan CommitEntry, cephadm *cephadm.Cephadm, monitor *Monitor, ready chan struct{}) *Consensus {
	instance := &Consensus{
		id:                 id,
		logs:               make([]*LogEntry, 0),
		cephadm:            cephadm,
		state:              Follower,
		currentTerm:        0,
		votedFor:           "",
		electionResetTime:  time.Now(),
		commitChan:         commitChan,
		monitor:            monitor,
		ready:              ready,
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
		newCommitReadyChan: make(chan struct{}),
		lastApplied:        -1,
		commitIndex:        -1,
	}

	go func() {
		select {
		case <-ready:
			log.Printf("mon(id:%s) consensus module ready", id)
		}
		// 初始化共识模块后状态为follower，故要开启选举倒计时
		go instance.runElectionTimer()
		// 初始化共识模块后持续监听可应用到状态机的日志
		go instance.commitChanSender()
	}()

	return instance
}

// electionTimeout 生成随机选举超时时间，150~300ms
// 需要明确一个定义，选举超时时间是指
//   - follow状态的节点经过electionTimeout时间后转化成candidate状态
//   - candidate状态的节点在electionTimeout时间内
//     ** 没有多数follower的支持
//     ** 没有收到其他leader的心跳包
//     ** 没有收到其他candidate的选票申请
//     后开启新一轮选举
func (c *Consensus) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// becomeFollower
// 成为follower的情况
// * leader状态发现别的leader发来的包
// * leader状态发现比自己任期高的包
// * candidate状态收到心跳包
// 外层调用者需要加锁
func (c *Consensus) becomeFollower(term int) {
	log.Printf("mon(id:%s) become follower, leader:%s", c.monitor.id, c.leaderId)
	c.state = Follower
	c.currentTerm = term
	c.electionResetTime = time.Now()
	c.votedFor = ""

	go c.runElectionTimer()
}

// startLeader
// 外层调用者需要加锁
func (c *Consensus) startLeader() {
	log.Printf("mon(id:%s) become leader", c.monitor.id)
	c.state = Leader
	peerIds := c.cephadm.GetMonitorIds()
	for _, peerId := range peerIds {
		c.nextIndex[peerId] = len(c.logs)
		c.matchIndex[peerId] = -1
	}
	go func() {
		// 每50ms发送一次心跳包
		ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
		counter := 0
		defer ticker.Stop()
		for {
			log.Printf("mon(id:%s) new round to send internal heartbeat", c.id)
			// 需要持续向其他节点发送心跳包
			go c.leaderSendHeartbeats()
			// 每500ms检查外部节点状态
			// 外部节点心跳包的发送频率为100ms一次
			counter++
			if counter >= 5 {
				log.Printf("mon(id:%s) new round to check external heartbeat", c.id)
				counter = 0
				// 需要持续监听集群外部节点心跳通信状态
				go c.leaderCheckExternalHeartbeatsState()
			}
			c.mu.Lock()
			if c.state != Leader {
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
			<-ticker.C
		}
	}()
}

// leaderCheckExternalHeartbeatsState
// 当节点角色切换为leader后，持续监听外部节点最后更新心跳时间的表
// 如果有超过指定时间还没收到心跳包的外部节点，则认为不可达，提交删除该节点记录的命令
func (c *Consensus) leaderCheckExternalHeartbeatsState() {
	c.mu.Lock()
	if c.state != Leader {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	states := c.monitor.GetNodeState()
	log.Printf("mon(id:%s) external node state: %v", c.id, states)
	now := time.Now()
	for id, lastHeartbeatTime := range states {
		// 超过2000ms还没有心跳包来，就认为这个外部节点不可达了
		if now.Sub(lastHeartbeatTime) > 2000*time.Millisecond {
			command := fmt.Sprintf(COMMAND_FORMAT, "DEL", "nodes", id)
			c.submit(command)
		}
	}
}

// startElection 外层调用者需要加锁
func (c *Consensus) startElection() {
	log.Printf("mon(id:%s) start election", c.monitor.id)
	c.state = Candidate
	c.currentTerm += 1
	savedCurrentTerm := c.currentTerm
	c.electionResetTime = time.Now()
	c.votedFor = c.id

	// 自己给自己投一票
	voteReceived := 1

	// 循环调用rpc给自己拉票
	peerIds := c.cephadm.GetMonitorIds()
	for _, peerId := range peerIds {
		go func(peerId string) {
			c.mu.Lock()
			if peerId == c.id {
				if voteReceived*2 >= len(peerIds)+1 {
					c.startLeader()
				}
				c.mu.Unlock()
				return
			}
			savedLastLogIndex, savedLastLogTerm := c.getLastLogIndexAndTerm()
			c.mu.Unlock()

			var reply RequestVoteReply
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  c.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			err := c.monitor.server.call(peerId, "PeersModule.RequestVote", args, &reply)
			if err != nil {
				log.Println(err)
				return
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			// 不是候选人就不能再选举
			// 有三种情况
			// * 变成follower：收到其他leader的心跳包或者收到更高任期消息
			// * 变成leader：选举成功
			// * 变成dead：...
			if c.state != Candidate {
				return
			}

			// 当出现比自己任期大的节点应该主动成为follower
			// 毕竟分布式系统十分混沌，网络情况也很复杂
			// 一个可能的case是锁一直被占着，导致被这个go routine抢到的时候
			// 当前节点已经经历candidate -> leader -> candidate的过程
			// 还有一个可能的case是发生网络分区，接收到请求的节点已经迭代多个任期
			if reply.Term > savedCurrentTerm {
				c.becomeFollower(reply.Term)
				return
			}

			if reply.Term == savedCurrentTerm && reply.VoteGranted {
				voteReceived += 1
				if voteReceived*2 >= len(peerIds)+1 {
					c.startLeader()
					return
				}
			}
		}(peerId)
	}

	// 选举开始后也要开一个倒计时
	go c.runElectionTimer()
}

// runElectionTimer follow和candidate状态开始运行选举倒计时
// 这个方法应该放到一个单独的协程运行
// 当state变为leader或者任期发生改变时结束
func (c *Consensus) runElectionTimer() {
	electionTimeout := c.electionTimeout()
	c.mu.Lock()
	savedCurrentTerm := c.currentTerm
	c.mu.Unlock()

	ticker := time.NewTicker(time.Duration(10) * time.Millisecond)
	defer ticker.Stop()
	for {
		// 10ms执行一次循环
		<-ticker.C

		c.mu.Lock()
		// 这里很好理解，状态发生了改变（成为leader或者dead）停止计时，因为这两种状态不需要启动选举计时
		if c.state != Follower && c.state != Candidate {
			c.mu.Unlock()
			return
		}

		// * 当前节点成为候选人，任期加一
		// * 有其他节点成为候选人，当前节点成为follower，并且开启新的计时器，老计时器可以销毁
		if c.currentTerm > savedCurrentTerm {
			c.mu.Unlock()
			return
		}

		// 如果计时器时间超过了随机生成的选举倒计时，就开启新一轮选举
		if time.Since(c.electionResetTime) >= electionTimeout {
			c.startElection()
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

func (c *Consensus) leaderSendHeartbeats() {
	c.mu.Lock()
	// 可能在leader状态收到其他follow任期更高的回复（网络分区故障）
	// 可能在leader状态收到另一个leader的请求
	if c.state != Leader {
		c.mu.Unlock()
		return
	}
	savedCurrentTerm := c.currentTerm
	id := c.id
	c.mu.Unlock()

	// 循环给其他节点发送心跳包
	peerIds := c.cephadm.GetMonitorIds()
	for _, peerId := range peerIds {
		// 跳过自己
		if peerId == c.id {
			continue
		}
		// 设置局部变量，防止loop-closure问题
		go func(peerId string) {
			// 构造请求参数
			c.mu.Lock()
			// 获取peerId指向实例的下一个待保存的logEntry
			nextIndex := c.nextIndex[peerId]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			// 如果prevLogIndex小于0这里直接取会panic
			if prevLogIndex >= 0 {
				prevLogTerm = c.logs[prevLogIndex].Term
			}
			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      c.logs[nextIndex:],
				LeaderCommit: c.commitIndex,
			}
			c.mu.Unlock()

			reply := &AppendEntriesReply{}
			err := c.monitor.server.call(peerId, "PeersModule.AppendEntries", args, &reply)
			// 如果请求发送失败，认为是对方不可用，更新集群状态
			if err != nil {
				log.Println(err)
				return
			}

			// 当发生分区时会出现这种情况
			// 另一个分区的节点因为个数不够因此无法成为新leader
			// 因此会一直是candidate并且不断选举，把任期刷的很高
			// 当分区恢复时，leader发现有任期比自己高的节点，于是退化成follower
			c.mu.Lock()
			defer c.mu.Unlock()
			if reply.Term > savedCurrentTerm {
				c.becomeFollower(reply.Term)
			}

			if c.state == Leader && reply.Term == savedCurrentTerm {
				if reply.Success {
					// 更新peerId的下一个待存储logEntry信息
					c.nextIndex[peerId] = nextIndex + len(args.Entries)
					c.matchIndex[peerId] = c.nextIndex[peerId] - 1

					// 判断已经成功存储的节点数量是否满足应用提交的要求
					savedCommitIndex := c.commitIndex
					for i := savedCommitIndex + 1; i < len(c.logs); i++ {
						if c.logs[i].Term == c.currentTerm {
							matchCount := 1
							for _, peerId := range peerIds {
								// 上方收到对方成功存储logEntry的响应后更新matchIndex
								// 这里比较matchIndex是要得出一个当前已经符合应用条件的节点个数
								// 个数超过一半就可以应用日志
								if c.matchIndex[peerId] >= i {
									matchCount++
								}
							}
							if matchCount*2 > len(peerIds)+1 {
								c.commitIndex = i
							}
						}
					}

					if c.commitIndex != savedCommitIndex {
						log.Printf("[ATTENTION] mon(id:%s) commit logEntry", c.id)
						c.newCommitReadyChan <- struct{}{}
					}
				} else {
					c.nextIndex[peerId] = nextIndex - 1
					log.Printf("[ATTENTION] mon(id:%s) reply from %s !success", c.id, peerId)
				}
			}
		}(peerId)
	}
}

func (c *Consensus) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state = Dead
}

func (c *Consensus) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Dead {
		return nil
	}
	// 候选人的任期大于当前任期是常见的case
	// 这时只需要成为候选人的follower即可
	if args.Term > c.currentTerm {
		c.leaderId = ""
		c.becomeFollower(args.Term)
	}

	// * 自身是candidate，由于已经投票给自己，所以无法投票给另一个候选者
	// * 自身是follower（之前可能是leader，但在上面已经将状态转化成follower）
	// 	 ** 已经投票给一个候选者，则不投票
	//   ** 没有投票给任何人，投给该候选者
	lastLogIndex, lastLogTerm := c.getLastLogIndexAndTerm()

	if c.currentTerm == args.Term && (c.votedFor == "" || c.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		reply.VoteGranted = true
		c.votedFor = args.CandidateId
		c.electionResetTime = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = c.currentTerm
	return nil
}

func (c *Consensus) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Dead {
		return nil
	}

	c.leaderId = args.LeaderId
	if args.Term > c.currentTerm {
		c.becomeFollower(args.Term)
	}

	if args.Term == c.currentTerm {
		if c.state != Follower {
			c.becomeFollower(args.Term)
		}
		c.electionResetTime = time.Now()

		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(c.logs) && args.PrevLogTerm == c.logs[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			// 找到第一个应该append的logEntry
			// 这里是为了重复append
			for {
				// 能插入的都插入了
				if logInsertIndex >= len(c.logs) || newEntriesIndex >= len(args.Entries) {
					break
				}

				if c.logs[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}

				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				c.logs = append(c.logs[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			}

			if args.LeaderCommit > c.commitIndex {
				c.commitIndex = min(len(c.logs)-1, args.LeaderCommit)
				c.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = c.currentTerm
	return nil
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// commitChanSender
// 在共识模块初始化时启动一个协程持续监听newCommitReadyChan
// 当newCommitReadyChan有消息传入时说明当前节点已经被允许应用一些日志
// 即将被应用的日志通过commitChanSender传出共识模块
func (c *Consensus) commitChanSender() {
	for range c.newCommitReadyChan {
		log.Printf("[ATTENTION] mon(id:%s) recv log commit signal", c.id)
		c.mu.Lock()
		savedTerm := c.currentTerm
		savedLastApplied := c.lastApplied
		var entries []*LogEntry
		// 如果可提交索引领先已经提交索引，意味着这部分是将要应用到状态机的log
		if c.commitIndex > savedLastApplied {
			entries = c.logs[savedLastApplied+1 : c.commitIndex+1]
			c.lastApplied = c.commitIndex
		}
		c.mu.Unlock()

		for idx, logEntry := range entries {
			c.commitChan <- CommitEntry{
				Command: logEntry.Command,
				Index:   savedLastApplied + 1 + idx,
				Term:    savedTerm,
			}
		}
	}
}

// getLastLogIndexAndTerm
// 返回日志列表最后一个实体日志的索引和任期
// 如果日志列表长度为0，返回-1和-1
func (c *Consensus) getLastLogIndexAndTerm() (int, int) {
	if l := len(c.logs); l == 0 {
		return -1, -1
	} else {
		return l - 1, c.logs[l-1].Term
	}
}

// submit
// 客户端提交命令到共识模块
func (c *Consensus) submit(command interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 只有在leader状态提交才允许
	// 显而易见
	if c.state == Leader {
		c.logs = append(c.logs, &LogEntry{
			Command: command,
			Term:    c.currentTerm,
		})
		return true
	}
	return false
}

// report 上报当前节点的id、状态、任期
func (c *Consensus) report() (string, State, int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.id, c.state, c.currentTerm, c.leaderId
}

// PeersRPCProxy mon集群中对等体间调用的rpc方法
// 只涉及分布式共识（心跳包含在内）
type PeersRPCProxy struct {
	c *Consensus
}

func NewPeersRPCProxy(c *Consensus) *PeersRPCProxy {
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
	log.Printf("mon(id:%s) received rpc call %s from %s, args:%v", p.c.id, "RequestVote", args.CandidateId, args)
	return p.c.RequestVote(args, reply)
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId string

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries 心跳包 & 追加日志
func (p *PeersRPCProxy) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	log.Printf("mon(id:%s) received rpc call %s from %s, args:%v", p.c.id, "AppendEntries", args.LeaderId, args)
	return p.c.AppendEntries(args, reply)
}
