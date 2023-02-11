package consts

type RaftRole int64

const (
	RAFT_ROLE_CANIDATE = 1
	RAFT_ROLE_FOLLOWER = 2
	RAFT_ROLE_LEADER   = 3
)
