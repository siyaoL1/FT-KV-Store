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

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
)

// ***********************************
// ******** Struct Deinitions ********
// ***********************************

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	numPeers        int
	persister       *Persister // Object to hold this peer's persisted state
	me              int        // this peer's index into peers[]
	dead            int32      // set by Kill()
	status          Status
	leaderID        int
	lastContactTime time.Time

	// other
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Persistent state
	currentTerm int
	votedFor    int
	LogRecord   LogRecord

	LastIncludedIndex int // last index and term included in Snapshot
	LastIncludedTerm  int // Means we don't have that index in current LogRecord

	// Volatile state(note: volatile means subject to rapid or unpredictable change)
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// ***********************************
// ********  Utility Function ********
// ***********************************

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.status == LEADER
}

// If other server has higher term number, update term and become follower.
// Note that this function assumes to be called within a locked region
func (rf *Raft) checkTermNumebrL(otherTerm int) bool {
	if otherTerm > rf.currentTerm {
		Debug(dTerm, "S%d T%d, Converted from term %v to term %v, now a follower.\n", rf.me, otherTerm, rf.currentTerm, otherTerm)
		rf.currentTerm = otherTerm
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.persist(nil)
		return true
	}
	return false
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.status == LEADER

	if isLeader {
		newLog := Entry{Term: rf.currentTerm, Command: command}
		rf.LogRecord.append(newLog)
		rf.matchIndex[rf.me] += 1
		rf.persist(nil)
		Debug(dClient, "S%d T%d, Start || Received and appended 1 log entry, lastLogIndex: %v.\n", rf.me, rf.currentTerm, rf.lastLogIndex())
	}

	return rf.lastLogIndex(), rf.currentTerm, isLeader
}

// ************************************
// ******** Background Routine ********
// ************************************
// Note: When you need to lock part of the code, sometimes it's simpler just to
// take that part out as a separate function.
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == LEADER {
		rf.lastContactTime = time.Now()
		go rf.broadcastLogsL()
	}
	if rf.timedOut() {
		rf.lastContactTime = time.Now()
		rf.startElectionL()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		rf.tick()
		ms := 10 // Wait for 0.1 seconds
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dCommit, "S%d T%d, Prepare to apply!!! rf.lastApplied:%v, rf.commitIndex:%v, rf.lastApplied+1:%v, rf.lastLogIndex():%v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.lastApplied+1, rf.lastLogIndex())

	var msg ApplyMsg
	if rf.lastApplied < rf.LastIncludedIndex {
		rf.lastApplied = rf.LastIncludedIndex
		msg = ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.LastIncludedTerm,
			SnapshotIndex: rf.LastIncludedIndex,
		}
		Debug(dSnap, "S%d T%d, || Applying Snapshot with SnapshotTerm: %v, SnapshotIndex: %v.\n", rf.me, rf.currentTerm, msg.SnapshotTerm, msg.SnapshotIndex)
	} else if rf.lastApplied < rf.commitIndex && rf.lastApplied+1 <= rf.lastLogIndex() {
		rf.lastApplied += 1
		msg = ApplyMsg{
			CommandValid: true,
			Command:      rf.LogRecord.entry(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		// Debug(dLog2, "S%d T%d, before waiting for lock\n", rf.me, rf.currentTerm)
		// Debug(dLog2, "S%d T%d, waiting for lock\n", rf.me, rf.currentTerm)
		Debug(dCommit, "S%d T%d, || Applying logs with index: %v, command: %v, commitIndex: %v.\n", rf.me, rf.currentTerm, rf.lastApplied, msg.Command, rf.commitIndex)
		Debug(dCommit, "S%d T%d, || logs: %v.\n", rf.me, rf.currentTerm, rf.LogRecord)
		// rf.persist(nil)
	}
	if msg.CommandValid || msg.SnapshotValid {
		rf.mu.Unlock()
		// Debug(dCommit, "S%d T%d, Sending msg to applyCh:%v", rf.me, rf.currentTerm, rf.applyCh)
		rf.applyCh <- msg
		// Debug(dCommit, "S%d T%d, Sent!!!", rf.me, rf.currentTerm)
		rf.mu.Lock()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		// Your code here (2A)
		rf.apply()
		ms := 10 // Wait for 0.1 seconds
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
		mu:              sync.Mutex{},
		peers:           peers,
		numPeers:        len(peers),
		persister:       persister,
		me:              me,
		dead:            0,
		status:          FOLLOWER,
		leaderID:        -1,
		lastContactTime: time.Now(), // Maybe change to setElectionTime
		applyCh:         applyCh,

		// Persistent state on all server
		currentTerm:       0,
		votedFor:          -1,
		LogRecord:         mkLogEmpty(0),
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		// Volatile state on all servers
		commitIndex: 0,
		lastApplied: 0,
		// Volatile state on leader
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	// Your initialization code here (2A, 2B, 2C).
	// Conditional variable for apply channel
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Snapshots contains only commited and applied index
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex

	go rf.applier()
	// start ticker goroutine to start elections in background
	go rf.ticker()

	return rf
}

// ***********************************
// ********        Kill  	  ********
// ***********************************

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
