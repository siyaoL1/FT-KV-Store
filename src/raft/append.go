package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int // Index of log entry immediately preceding new logs
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictFirst int
	ConflictValid bool
}
