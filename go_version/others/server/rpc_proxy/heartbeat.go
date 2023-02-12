package rpc_proxy

type Heartbeats struct {
}

type HeartBeatArgs struct {
}

type HeartBeatReply struct {
}

func (h *Heartbeats) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	return nil
}
