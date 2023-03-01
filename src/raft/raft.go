package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ***************************
// **** Struct Deinitions ****
// ***************************

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int64

const (
	FOLLOWER  Status = 0
	CANDIDATE Status = 1
	LEADER    Status = 2
)

// Election related info
type Election struct {
	votesNumber map[int]int
	votedFor    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	numPeers        int
	persister       *Persister // Object to hold this peer's persisted state
	me              int        // this peer's index into peers[]
	dead            int32      // set by Kill()
	currentTerm     int
	status          Status
	leaderID        int
	election        Election
	lastContactTime time.Time

	// other

	log         []interface{}
	commitIndex int
	// lastApplied int
	// nextIndex  []int
	// matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

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

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// prevLogIndex int
	// prevLogTerm  int
	// entries      []int
	// leaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success int
}

// ***************************
// ****  Leader Election  ****
// ***************************

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.status == LEADER
}

// Clear data stored for a election process
// func (rf *Raft) clearElectionData() {
// 	rf.election.votedFor = -1
// }

// If other server has higher term number, update term and become follower.
// Note that this function assumes to be called within a locked region
func (rf *Raft) checkTermNumebr(otherTerm int) {
	if otherTerm > rf.currentTerm {
		rf.currentTerm = otherTerm
		rf.status = FOLLOWER
		rf.election.votedFor = -1
	}
}

// RequestVote RPC handler (being handled concurrently)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Figure 2
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.election.votedFor == -1 || rf.election.votedFor == args.CandidateID)) {
		// (args.LastLogIndex >= rf.commitIndex && args.LastLogIndex >= rf.currentTerm) { // 2B
		rf.lastContactTime = time.Now()
		rf.checkTermNumebr(args.Term)
		rf.election.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Yooo I don't vote for u, args.Term: %v, rf.currentTerm: %v, rf.election.votedFor: %v, args.CandidateID: %v\n", args.Term, rf.currentTerm, rf.election.votedFor, args.CandidateID)
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("(Server %v, term: %v) sending RequestVote RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		DPrintf("Failed in RequestVote PRC from %v to %v! (term: %v)\n", rf.me, server, args.Term)
		return
	}

	DPrintf("(Server %v, term: %v) received RequestVote reply from %v, granted: %v\n", rf.me, reply.Term, server, reply.VoteGranted)

	rf.mu.Lock()
	rf.checkTermNumebr(reply.Term)
	// Count the number of votes
	if reply.VoteGranted {
		rf.election.votesNumber[reply.Term] += 1
	}
	rf.mu.Unlock()
}

// Starts a go routine in the background that sends RequestVote RPC to all servers
func (rf *Raft) startElection() {
	DPrintf("(Server %v, term: %v) Started election\n", rf.me, rf.currentTerm+1)

	// Before election info update
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.election.votesNumber[rf.currentTerm] = 1
	rf.election.votedFor = rf.me
	rf.lastContactTime = time.Now()
	rf.mu.Unlock()

	// Send each server a RequestVote RPC
	for server := 0; server < rf.numPeers; server++ {
		if server == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.commitIndex,
			LastLogTerm:  rf.currentTerm,
		}
		reply := RequestVoteReply{}
		// Create goroutines to send RequestVote RPC
		go rf.sendRequestVote(server, &args, &reply)
	}
}

// Helper function to check whether it is timed out from the base and random milisecond duration
// 550 to 850 milliseconds
func (rf *Raft) timedOut() bool {
	base := int64(550)
	random := int64(300)
	duration := time.Duration(base+(rand.Int63()%random)) * time.Millisecond
	DPrintf("(Server %v, term: %v) election timeout duration is: %v, duration since start is :%v, timed out: %v\n", rf.me, rf.currentTerm, duration, time.Since(rf.lastContactTime), time.Since(rf.lastContactTime) > duration)
	return time.Since(rf.lastContactTime) > duration
}

// ***************************
// ****     Heartbeat  	  ****
// ***************************

// RequestVote RPC handler (being handled concurrently)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = 0
		return
	}

	reply.Success = 1
	rf.lastContactTime = time.Now()
	rf.leaderID = args.LeaderID

	rf.currentTerm = args.Term
	rf.status = FOLLOWER
	// rf.checkTermNumebr(args.Term)
	rf.election.votedFor = -1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("(Server %v, term: %v) sending AppendEntries RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		DPrintf("Failed in AppendEntries PRC from %v to %v! (term: %v)\n", rf.me, server, args.Term)
		return
	}

	rf.checkTermNumebr(reply.Term)
	// DPrintf("(Server %v, term: %v) received AppendEntries reply from %v (term %v)\n", rf.me, reply.Term, server)
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("(Server %v, term: %v) || Broadcasting heartbeats ||\n", rf.me, rf.currentTerm)
	for server := 0; server < rf.numPeers; server++ {
		if server == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		reply := AppendEntriesReply{}

		go rf.sendAppendEntries(server, &args, &reply)
	}
}

// ***************************
// **** Background Check  ****
// ***************************

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		switch rf.status {
		case FOLLOWER: // Follower only waits to receive RPC, if timedout start eleciton
			if rf.timedOut() {
				rf.mu.Lock()
				rf.status = CANDIDATE
				rf.mu.Unlock()
				go rf.startElection()
			}

		case CANDIDATE: // Candidates checks if it becomes leader, if timeout start election again
			if rf.timedOut() {
				go rf.startElection()
			}
			rf.mu.Lock()
			if rf.election.votesNumber[rf.currentTerm] > rf.numPeers/2 {
				rf.status = LEADER
				rf.election.votedFor = -1
			}
			rf.mu.Unlock()

		case LEADER: // Leader sends out heatbeat to inform other servers
			rf.broadcastHeartbeat()
			// Wait for 0.1 seconds to send heartbeat again
			// time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		numPeers:    len(peers),
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		status:      FOLLOWER,
		election: Election{
			votesNumber: map[int]int{}, // TODO: Change this back to int later.
			votedFor:    -1,
		},
		lastContactTime: time.Now(),
		commitIndex:     0,
	}

	// Your initialization code here (2A, 2B, 2C).
	// rf.election =

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections in background
	go rf.ticker()

	return rf
}

// ***************************
// ****        Log  	  ****
// ***************************

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// ***************************
// ****        Kill  	  ****
// ***************************

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ***************************
// ****     Persistent 	  ****
// ***************************

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
