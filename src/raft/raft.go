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
	"log"
	"reflect"
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
	currentTerm     int
	status          Status
	leaderID        int
	lastContactTime time.Time

	// other
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	votedFor  int
	log       Log

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

// ***********************************
// ********  Utility Function ********
// ***********************************

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
func (rf *Raft) checkTermNumebrL(otherTerm int) {
	if otherTerm > rf.currentTerm {
		DPrintf("(Server %v, term: %v) Converted from term %v to term %v, now a follower.\n", rf.me, otherTerm, rf.currentTerm, otherTerm)
		rf.currentTerm = otherTerm
		rf.status = FOLLOWER
		rf.votedFor = -1
		// rf.persist()
	}
}

// ***********************************
// ******** Log and Heartbeat ********
// ***********************************

// AppendEntries RPC handler (being handled concurrently)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("(Server %v, term: %v) || Received AppendEntry RPC ||\n", rf.me, rf.currentTerm)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all server
	rf.checkTermNumebrL(args.Term)

	if rf.currentTerm > args.Term { // invalid term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastContactTime = time.Now()
	reply.Success = false

	// Checks if the current log is inconsistent from leader's
	if args.PrevLogIndex > rf.log.lastLogIndex() { // invalid prev log index
		// Tell leader to backup nextIndex[]
		reply.ConflictTerm = args.Term + 1
		reply.ConflictFirst = rf.log.lastLogIndex() + 1
		reply.ConflictValid = true
	} else if args.PrevLogIndex < rf.log.start() {
		reply.ConflictTerm = args.Term + 1
		reply.ConflictFirst = rf.log.lastLogIndex() + 1
		reply.ConflictValid = true
	} else if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// conflict entry, need to tell leader to roll back
		reply.ConflictTerm = rf.log.lastLogEntry().Term
		reply.ConflictFirst = rf.log.lastLogIndex()
		reply.ConflictValid = true
		// Roll back the entire term
		i := rf.log.lastLogIndex()
		for i > rf.log.start()+1 && rf.log.entry(i).Term == reply.ConflictTerm {
			reply.ConflictFirst = i
			i -= 1
		}
	} else {
		rf.appendEntresL(args, reply)
		reply.Success = true
	}
	rf.signalApplierL()
	reply.Term = rf.currentTerm

	// // TODO: Stopped here, the condition isn't correct
	// // Clear the logs starting with args.PrevLogIndex
	// if rf.lastLogIndex >= args.PrevLogIndex && realIndex(args.PrevLogIndex) >= 0 {
	// 	DPrintf("(Server %v, term: %v) || Removing invalid log entries ||\n", rf.me, rf.currentTerm)
	// 	// TODO: define a generic function for this instead.
	// 	// Here we directly use prevLogIndex since if it is 0, it will have no element. if 1, have 1 element.
	// 	DPrintf("(Server %v, term: %v) || Deleted old logs from %v to %v.||\n", rf.me, rf.currentTerm, rf.log, rf.log[:args.PrevLogIndex])
	// 	rf.log = rf.log[:args.PrevLogIndex]
	// 	rf.lastLogIndex = len(rf.log)
	// 	if rf.lastLogIndex != 0 {
	// 		rf.lastLogTerm = rf.log[realIndex(rf.lastLogIndex)].Term
	// 	} else {
	// 		rf.lastLogTerm = 0
	// 	}
	// }

	// // Append all the log entries
	// if len(args.Entries) > 0 {
	// 	rf.appendLogs(args.Entries)
	// }

	// // Update commitIndex
	// if args.LeaderCommit > rf.commitIndex {
	// 	DPrintf("(Server %v, term: %v) || Updated commitIndex from %v to %v.||\n", rf.me, rf.currentTerm, rf.commitIndex, min(args.LeaderCommit, rf.lastLogIndex))
	// 	rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
	// }

	// DPrintf("(Server %v, term: %v) || Current log: %v.||\n", rf.me, rf.currentTerm, rf.log)

	// reply.Success = true
	// rf.leaderID = args.LeaderID
}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) appendEntresL(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.status == CANDIDATE {
		rf.status = FOLLOWER
	}

	// Rule 3
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		// If two logs have the same index and same term they are identical,
		// this case they don't have the same term so we erase the log starting from that index.
		if index <= rf.log.lastLogIndex() && rf.log.entry(index).Term != entry.Term {
			rf.log.cutend(index)
		}
	}

	// Rule 4
	for i, entry := range args.Entries {
		// The index that new entry will be appended at
		index := args.PrevLogIndex + 1 + i
		if index == rf.log.lastLogIndex()+1 {
			rf.log.append(entry)
		}
		if reflect.DeepEqual(rf.log.entry(index).Command, entry.Command) == false {
			log.Fatal("Entry error %v from=%v index%v old=%v new=%v\n",
				rf.me, args.LeaderID, index, rf.log.entry(index), args.Entries[i])
		}
	}

	// Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		lastNew := args.PrevLogIndex + 1 + len(args.Entries) - 1
		if rf.commitIndex > lastNew {
			DPrintf("(Server %v, term: %v) || Commit || Commit to %v\n", rf.me, rf.currentTerm, lastNew)
		}
	}

	// Write log to stable storage
	// rf.persist()
}

func (rf *Raft) processAppendReplyL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Handle AppendEntries result
	DPrintf("(Leader %v, term: %v) || Response || Received AppendEntries response from %v with success: %v \n", rf.me, rf.currentTerm, server, reply.Success == 1)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success { // Success
		// Update nextIndex and matchIndex
		numEntries := len(args.Entries)
		if len(args.Entries) != 0 {
			rf.nextIndex[server] = args.PrevLogIndex + numEntries + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			DPrintf("(Leader %v, term: %v) || Response || Updated server %v with nextIndex: %v, matchIndex: %v \n", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])

			// Update commitIndex
			newMatchIndex := rf.matchIndex[server]
			if newMatchIndex > rf.commitIndex {
				// Find the number of server that has higher matchIndex
				count := 0
				for i := 0; i < rf.numPeers; i++ {
					if rf.matchIndex[i] >= newMatchIndex {
						count += 1
					}
				}
				// If majority, updates commitIndex
				if count > rf.numPeers/2 {
					DPrintf("(Leader %v, term: %v) || Updated commitIndex from %v to %v.||\n", rf.me, rf.currentTerm, rf.commitIndex, newMatchIndex)
					rf.commitIndex = newMatchIndex
				}
			} else {
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		}
	} else { // Failure
		if reply.Term >= args.Term {
			rf.nextIndex[server] -= 1
			DPrintf("(Leader %v, term: %v) || Response || Decremented server %v's nextIndex to %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server])
		}
		// Check and update term number
		rf.checkTermNumebrL(reply.Term)
	}
	// DPrintf("(Server %v, term: %v) received AppendEntries reply from %v (term %v)\n", rf.me, reply.Term, server)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("(Server %v, term: %v) sending AppendEntries RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		DPrintf("Failed in AppendEntries PRC from %v to %v! (term: %v)\n", rf.me, server, args.Term)
	}
	return ok
}

func (rf *Raft) sendAppendL(server int, heartbeat bool) {

	prevLogIndex := 0
	prevLogTerm := 0

	if realIndex(rf.nextIndex[server]-1) >= 0 {
		prevLogIndex = rf.nextIndex[server] - 1 // prevIndex = nextIndex - 1
		prevLogTerm = rf.log.entry(rf.nextIndex[server] - 1).Term
	}

	// Construct Args for AppendEntries RPC
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}

	if rf.log.lastLogIndex() >= rf.nextIndex[server] {
		// Sending logs
		args.Entries = make([]Entry, rf.log.lastLogIndex()-rf.nextIndex[server]+1)
		// starting from the nextIndex which is prevIndex + 1
		copy(args.Entries, rf.log.slice(rf.nextIndex[server]))
	}
	DPrintf("(Leader %v, term: %v) || Send || Replicating logs to %v, log length: %v, PrevLogIndex: %v, PrevLogTerm:%v, LeaderCommit: %v \n", rf.me, rf.currentTerm, server, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)

		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReplyL(server, &args, &reply)
		}
	}()
}

func (rf *Raft) broadcastLogsL(heartbeat bool) {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		if rf.lastLogIndex >= rf.nextIndex[server] || heartbeat {
			// Sending logs
			rf.sendAppendL(server, heartbeat)
		}
		DPrintf("(Leader %v, term: %v) || Send || Replicating logs to %v, log length: %v, PrevLogIndex: %v, PrevLogTerm:%v, LeaderCommit: %v \n", rf.me, rf.currentTerm, server, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

		reply := AppendEntriesReply{}

		go rf.sendAppendEntries(server, &args, &reply)
	}

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func realIndex(index int) int {
	return index - 1
}

// Assumes that the lock is already obtained when call this function.
func (rf *Raft) appendLogs(logs []LogEntry) {
	rf.log = append(rf.log, logs...)
	rf.lastLogIndex += len(logs)
	rf.lastLogTerm = rf.log[realIndex(rf.lastLogIndex)].Term
	DPrintf("(Server %v, term: %v) || Appended %v log entries, lastLogIndex: %v, lastLogTerm: %v ||\n", rf.me, rf.currentTerm, len(logs), rf.lastLogIndex, rf.lastLogTerm)
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
	isLeader := rf.status == LEADER

	if isLeader {
		newLog := Entry{Term: rf.currentTerm, Command: command}
		rf.mu.Lock()
		rf.appendLogs([]Entry{newLog})
		rf.matchIndex[rf.me] += 1
		rf.mu.Unlock()
		DPrintf("(Leader %v, term: %v) Received a command, added to index: %v\n", rf.me, rf.currentTerm, rf.lastLogIndex)
	}

	return rf.lastLogIndex, rf.currentTerm, isLeader
}

// ***********************************
// ******** Background Check  ********
// ***********************************
// Note: When you need to lock part of the code, sometimes it's simpler just to take that part out as a separate function.
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == LEADER {
		rf.lastContactTime = time.Now()
		go rf.broadcastLogs()
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
		ms := 100 // Wait for 0.1 seconds
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	// TODO: Stopped here need to update applier thread
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[realIndex(rf.lastApplied+1)].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.applyCh <- msg
		rf.lastApplied += 1

		DPrintf("(Server %v, term: %v) || Applying logs with index: %v ||\n", rf.me, rf.currentTerm, rf.lastApplied+1)
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
		currentTerm:     0,
		status:          FOLLOWER,
		leaderID:        -1,
		votedFor:        -1,
		lastContactTime: time.Now(), // Maybe change to setElectionTime
		applyCh:         applyCh,
		log:             mkLogEmpty(),
		commitIndex:     0,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
	}
	// Your initialization code here (2A, 2B, 2C).
	// Conditional variable for apply channel
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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

// ***********************************
// ********     Persistent 	  ********
// ***********************************

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
