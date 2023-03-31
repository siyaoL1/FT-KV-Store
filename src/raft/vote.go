package raft

import (
	"math/rand"
	"time"
)

type Status int64

const (
	FOLLOWER  Status = 0
	CANDIDATE Status = 1
	LEADER    Status = 2
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// ***********************************
// ********  Leader Election  ********
// ***********************************

// RequestVote RPC handler (being handled concurrently)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTermNumebrL(args.Term)

	// Figure 2
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID { // haven't vote or vote for it already
		if (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) ||
			args.LastLogTerm > rf.lastLogTerm() {
			rf.lastContactTime = time.Now()
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		}
	}
	rf.persist(nil)

	Debug(dVote, "S%d T%d, || logs: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.Log)
	if !reply.VoteGranted {
		Debug(dVote, "S%d T%d, Rejected vote to %d, rf.currentTerm: %v, rf.election.votedFor: %v, rf.lastLogTerm: %v, rf.lastLogIndex: %v, args.LastLogTerm: %v, args.LastLogIndex: %v\n", rf.me, rf.currentTerm, args.CandidateID, rf.currentTerm, rf.votedFor, rf.lastLogTerm(), rf.lastLogIndex(), args.LastLogTerm, args.LastLogIndex)
	} else {
		Debug(dVote, "S%d T%d, Granted vote to %d, rf.currentTerm: %v, rf.election.votedFor: %v, args.LastLogTerm: %v, args.LastLogIndex: %v\n", rf.me, rf.currentTerm, args.CandidateID, rf.currentTerm, rf.votedFor, args.LastLogTerm, args.LastLogIndex)
	}
}

func (rf *Raft) becomeLeaderL() {
	if rf.status == LEADER {
		return
	}
	Debug(dVote, "S%d T%d, Became a leader \n", rf.me, rf.currentTerm)
	rf.status = LEADER

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.broadcastLogsL()

	// old code
	// if rf.election.votesNumber[rf.currentTerm] > rf.numPeers/2 {
	// 	Debug(dVote, "S%d T%d, || Wins Election  ||\n", rf.me, rf.currentTerm)
	// 	rf.status = LEADER
	// 	rf.election.votedFor = -1
	// 	for i := 0; i < len(rf.nextIndex); i++ {
	// 		rf.nextIndex[i] = rf.lastLogIndex + 1
	// 		rf.matchIndex[i] = rf.lastLogIndex + 1
	// 	}
	// }
}

// Send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	Debug(dVote, "S%d T%d, sending RequestVote RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) requestVotes(server int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, args, &reply)

	if !ok {
		Debug(dVote, "S%d T%d, Failed in RequestVote PRC from %v to %v! (term: %v)\n", rf.me, rf.currentTerm, rf.me, server, args.Term)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d T%d, received RequestVote reply from %v, granted: %v\n", rf.me, reply.Term, server, reply.VoteGranted)

	rf.checkTermNumebrL(reply.Term)
	// Count the number of votes
	if reply.VoteGranted {
		*votes += 1
		if *votes > rf.numPeers/2 {
			if rf.currentTerm == args.Term {
				rf.becomeLeaderL()
				Debug(dLeader, "S%d T%d, Leader logs: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.Log)
			}
		}
	}
}

func (rf *Raft) requestVotesL() {
	// There's only one args and vote being shared among all RequstVote RPCs.
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	votes := 1
	Debug(dVote, "S%d T%d, || logs: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.Log)
	// Send each server a RequestVote RPC
	for server := 0; server < rf.numPeers; server++ {
		if server != rf.me {
			go rf.requestVotes(server, &args, &votes) // Send RPCs in parallel
		}
	}
}

// Starts a go routine in the background that sends RequestVote RPC to all servers
func (rf *Raft) startElectionL() {
	Debug(dVote, "S%d T%d, Started election\n", rf.me, rf.currentTerm+1)

	// Before election info update
	rf.currentTerm += 1
	rf.status = CANDIDATE
	rf.votedFor = rf.me
	rf.persist(nil)

	rf.requestVotesL()
}

// Helper function to check whether it is timed out from the base and random milisecond duration
// 550 to 850 milliseconds
func (rf *Raft) timedOut() bool {
	base := int64(550)
	random := int64(300)
	duration := time.Duration(base+(rand.Int63()%random)) * time.Millisecond
	timeout := time.Since(rf.lastContactTime) > duration
	// Debug(dVote, "S%d T%d, election timeout duration is: %v, duration since start is :%v, timed out: %v\n", rf.me, rf.currentTerm, duration, time.Since(rf.lastContactTime), time.Since(rf.lastContactTime) > duration)

	if timeout {
		Debug(dVote, "S%d T%d, timed out\n", rf.me, rf.currentTerm)
	}
	return timeout
}
