package raft

import (
	"bytes"

	"6.5840/labgob"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// ***************************************
// ********    Sending Snapshot   ********
// ***************************************

// Send a InstallSnapshot RPC to a follower
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	Debug(dSnap, "S%d T%d, sending InstallSnapshot RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installSnapshot(server int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	var reply InstallSnapshotReply

	ok := rf.sendInstallSnapshot(server, args, &reply)
	if !ok {
		Debug(dWarn, "S%v, Failed in InstallSnapshotReply PRC from %v to %v! (term: %v)\n", rf.me, rf.me, server, args.Term)
	}

	rf.mu.Lock()
	rf.processSnapshotReplyL(server, args, &reply)
	defer rf.mu.Unlock()
}

// *****************************************
// ******** InstallSnapshot Handler ********
// *****************************************
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Debug(dSnap, "S%d T%d, || Receive || Received InstallSnapshot RPC ||\n", rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// Rule 1
	if args.Term < rf.currentTerm {
		return
	}

	rf.checkTermNumebrL(args.Term)

	if args.LastIncludedIndex <= rf.LastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	nextIndex := args.LastIncludedIndex + 1
	if rf.lastLogIndex() <= args.LastIncludedIndex {
		// remove all logs
		rf.LogRecord = mkLog(make([]Entry, 0), nextIndex)
	} else {
		if rf.LogRecord.entry(args.LastIncludedIndex).Term != args.LastIncludedTerm {
			// remove all logs
			rf.LogRecord = mkLog(make([]Entry, 0), nextIndex)
		} else {
			// remove logs up include lastIncludedIndex
			rf.LogRecord = mkLog(rf.LogRecord.slice(nextIndex), nextIndex)
		}
	}

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// install Snapshot
	rf.persist(args.Data)

}

// ***************************************
// ********   Response Handler   *********
// ***************************************
func (rf *Raft) processSnapshotReplyL(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Debug(dSnap, "S%d T%d, received InstallSnapshot reply from %v, reply term: %v\n", rf.me, reply.Term, server, reply.Term)

	// Check term number from the reply
	rf.checkTermNumebrL(reply.Term)

	// the term is still the same
	if rf.currentTerm == args.Term {
		// only update if the lastInclude is indeed higher than nextIndex
		if args.LastIncludedIndex+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		Debug(dSnap, "S%d T%d, Leader|| updated server: %v nextIndex to %v, matchIndex to:%v.\n", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
	}
}

// ***************************************
// ********    Create Snapshot   *********
// ***************************************

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	firstIndex := rf.LogRecord.Index0

	// Nothing to snapshot
	if index <= firstIndex || index > rf.lastLogIndex() {
		return
	}
	Debug(dSnap, "S%d T%d, || Before snapshot: index: %v, log: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.Index0, rf.LogRecord.Log)

	rf.LastIncludedTerm = rf.LogRecord.entry(index).Term
	rf.LogRecord.Log = rf.LogRecord.slice(index)
	rf.LastIncludedIndex = index
	rf.LogRecord.Index0 = index

	rf.persist(snapshot)
	Debug(dSnap, "S%d T%d, || After snapshot: index: %v, log: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.Index0, rf.LogRecord.Log)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  rf.LastIncludedTerm,
		SnapshotIndex: rf.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
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
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.LogRecord)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	raftstate := w.Bytes()

	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.LogRecord)
	d.Decode(&rf.LastIncludedIndex)
	d.Decode(&rf.LastIncludedTerm)
}
