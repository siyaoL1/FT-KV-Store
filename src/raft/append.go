package raft

import (
	"log"
	"reflect"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int // Index of log entry immediately preceding new logs
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int // term in the conflicting entry
	XFirst   int // index of first entry with that term
	XLen     int // log length, which should be the last log's index
}

// ***************************************
// ******** Sending AppendEntries ********
// ***************************************

// Broadcasting AppendEntries RPCs to all followers. Covers both heartbeats
// and replciates logs
func (rf *Raft) broadcastLogsL() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[server] - 1
		var prevLogTerm int
		// Leader's log is too short, need to install Snapshot
		if prevLogIndex < rf.LastIncludedIndex {
			Debug(dSnap, "S%d T%d, leader's log is too short for %d, prevLogIndex:%d, rf.LastIncludedIndex:%d", rf.me, rf.currentTerm, server, prevLogIndex, rf.LastIncludedIndex)
			Debug(dSnap, "S%d T%d, rf.nextIndex:%v", rf.me, rf.currentTerm, rf.nextIndex)

			go rf.installSnapshot(server)
			continue
		} else if prevLogIndex == rf.LastIncludedIndex {
			prevLogTerm = rf.LastIncludedTerm
		} else {
			prevLogTerm = rf.LogRecord.term(rf.nextIndex[server] - 1)
		}

		// Construct Args for AppendEntries RPC
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}

		// When there's logs to replicate
		if rf.lastLogIndex() >= rf.nextIndex[server] {
			args.Entries = make([]Entry, rf.lastLogIndex()-rf.nextIndex[server]+1)
			// starting from the nextIndex which is prevIndex + 1
			copy(args.Entries, rf.LogRecord.slice(rf.nextIndex[server]))
		}

		Debug(dLeader, "S%d T%d, Send || Replicating logs to %v, log length: %v, PrevLogIndex: %v, PrevLogTerm:%v, LeaderCommit: %v \n", rf.me, rf.currentTerm, server, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

		// Go routine to send AppendEntries RPC and wait for response
		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.processAppendReplyL(server, &args, &reply)
			}
		}(server)
	}
}

// Send a AppendEntries RPC to a follower
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Debug(dLog, "S%d T%d, sending AppendEntries RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		Debug(dLog, "S%d T%d, Failed in AppendEntries PRC from %v to %v! (term: %v)\n", rf.me, rf.currentTerm, rf.me, server, args.Term)
	}
	return ok
}

// ***************************************
// ******** AppendEntries Handler ********
// ***************************************

// AppendEntries RPC request handler (being handled concurrently)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLog, "S%d T%d, Received AppendEntry RPC \n", rf.me, rf.currentTerm)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all server
	rf.checkTermNumebrL(args.Term)

	// Rule 1
	if rf.currentTerm > args.Term { // invalid term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastContactTime = time.Now()
	reply.Success = false

	// Checks if the current log is inconsistent from leader's
	// First case is Rule 2
	if args.PrevLogIndex > rf.lastLogIndex() { //
		// Log is too short, Tell leader to backup
		reply.XTerm = rf.lastLogTerm()
		reply.XFirst = rf.LogRecord.firstOfTerm(reply.XTerm, rf.lastLogIndex())
		reply.XLen = rf.lastLogIndex()
		reply.Conflict = true
		Debug(dLog, "S%d T%d, AppendEntry Conflict 1\n", rf.me, rf.currentTerm)
	} else if args.PrevLogIndex < rf.LogRecord.startIndex() {
		// invalid prev log index
		// prevLogIndex is being snapshotted.
		// Does this need to be considered? Since if a log is being snapshotted, we would not appendEntries at that index.
		Debug(dLog, "S%d T%d, AppendEntry Conflict 2\n", rf.me, rf.currentTerm)
	} else if (rf.LogRecord.len() > 1 && rf.LogRecord.term(args.PrevLogIndex) != args.PrevLogTerm) ||
		(rf.LogRecord.len() == 1 && rf.LastIncludedTerm != args.PrevLogTerm) {
		// conflict entry, need to tell leader to roll back
		reply.XTerm = rf.LogRecord.term(args.PrevLogIndex)
		reply.XFirst = rf.LogRecord.firstOfTerm(reply.XTerm, args.PrevLogIndex)
		reply.XLen = rf.lastLogIndex()
		reply.Conflict = true
		Debug(dLog, "S%d T%d, AppendEntry Conflict 3\n", rf.me, rf.currentTerm)
		Debug(dLog, "S%d T%d, || logs: %v, args: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.Log, args)
	} else {
		rf.updateLogL(args, reply)
		reply.Success = true
	}

	// Debug(dLog, "S%d T%d, || Current log: %v.||\n", rf.me, rf.currentTerm, rf.log)
	rf.leaderID = args.LeaderID

	rf.signalApplierL()
	reply.Term = rf.currentTerm
	Debug(dLog, "S%d T%d, Replied AppendEntry with %t, Conflict: %t\n", rf.me, rf.currentTerm, reply.Success, reply.Conflict)
}

func (rf *Raft) updateLogL(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.status == CANDIDATE {
		rf.status = FOLLOWER
	}

	// Rule 3
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		// If two logs have the same index and same term they are identical,
		// this case they don't have the same term so we erase the log starting from that index.
		if index <= rf.lastLogIndex() && rf.LogRecord.term(index) != entry.Term {
			(&(rf.LogRecord)).cutend(index)
		}
	}

	// Rule 4
	for i, entry := range args.Entries {
		// The index that new entry will be appended at
		index := args.PrevLogIndex + 1 + i
		if index == rf.lastLogIndex()+1 {
			rf.LogRecord.append(entry)
			Debug(dLog, "S%d T%d, || Append || Appended 1 log entry, lastLogIndex: %v.\n", rf.me, rf.currentTerm, rf.lastLogIndex())
		}
		if !reflect.DeepEqual(rf.LogRecord.entry(index).Command, entry.Command) {
			// Debug(dLog, "Entry error")
			Debug(dLog, "S%d T%d, || Current log index0: %v, log: %v.||\n", rf.me, rf.currentTerm, rf.LogRecord.Index0, rf.LogRecord)

			log.Fatalf("Entry error %v from=%v index%v old=%v new=%v\n",
				rf.me, args.LeaderID, index, rf.LogRecord.entry(index), args.Entries[i])
		}
	}

	// Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		latestIndex := args.PrevLogIndex + 1 + len(args.Entries) - 1
		Debug(dCommit, "S%d T%d, || Commit || Commit to %v\n", rf.me, rf.currentTerm, rf.commitIndex)
		if rf.commitIndex > latestIndex {
			Debug(dCommit, "S%d T%d, || Commit || Commit to %v\n", rf.me, rf.currentTerm, latestIndex)
			rf.commitIndex = latestIndex
		}
	}

	// Write log to stable storage
	rf.persist(nil)
}

// ***************************************
// ********   Response Handler   *********
// ***************************************

// AppendEntries RPC response Handler
func (rf *Raft) processAppendReplyL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Handle AppendEntries result
	Debug(dLeader, "S%d T%d, Response || Received AppendEntries response from %v with success: %v \n", rf.me, rf.currentTerm, server, reply.Success)
	if rf.currentTerm < reply.Term { // response term is higher than current term
		rf.checkTermNumebrL(reply.Term)
	} else if rf.currentTerm == args.Term { // term is still the same from sending the AppendEntries RPC
		if reply.Success { // Success
			// Update nextIndex and matchIndex
			numEntries := len(args.Entries)
			if numEntries != 0 {
				newNextIndex := args.PrevLogIndex + numEntries + 1
				newMatchIndex := newNextIndex - 1

				if newNextIndex > rf.nextIndex[server] {
					rf.nextIndex[server] = newNextIndex
				}
				if newMatchIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
				Debug(dLeader, "S%d T%d, Update || Updated server %v with nextIndex: %v, matchIndex: %v \n", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
			}
		} else if reply.Conflict { // Failure
			// Backup one term
			rf.handleconflictL(server, args, reply)
		}
		rf.advancedCommitL()
		// Debug(dLog, "S%d T%d, received AppendEntries reply from %v (term %v)\n", rf.me, reply.Term, server)

	}
}

func (rf *Raft) handleconflictL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// starting from conflictfirst going backward to find the last entry of the term if exist
	lastIndexOfTerm := rf.LogRecord.lastOfTerm(reply.XTerm, reply.XFirst)
	Debug(dLeader, "S%d T%d, Handling Append Conflict, XTerm: %v, XFirst: %v, log: %v\n", rf.me, rf.currentTerm, reply.XTerm, reply.XFirst, rf.LogRecord)
	newNextIndex := -1
	if lastIndexOfTerm == -1 {
		// Doesn't have term
		Debug(dLeader, "S%d T%d, Handling Append Conflict, Case 1 \n", rf.me, rf.currentTerm)
		newNextIndex = reply.XFirst
	} else {
		// Has Term
		Debug(dLeader, "S%d T%d, Handling Append Conflict, Case 2 \n", rf.me, rf.currentTerm)
		newNextIndex = lastIndexOfTerm
	}

	// Follower's log is too short
	if rf.nextIndex[server] > reply.XLen {
		Debug(dLeader, "S%d T%d, Handling Append Conflict, Case 3 \n", rf.me, rf.currentTerm)
		newNextIndex = reply.XLen
	}
	rf.nextIndex[server] = newNextIndex

	// // 2D
	// if rf.nextIndex[server] < rf.LogRecord.startIndex()+1 {
	// 	rf.sendSnaphsot()
	// }
}

// ***************************************
// ********   Update CommitIndex  ********
// ***************************************

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) advancedCommitL() {
	if rf.status != LEADER {
		log.Fatalf("advanceCommit: state %v\n", rf.status)
	}

	start := rf.commitIndex + 1
	if start < rf.LogRecord.startIndex() { // 2D: when restart, start could be 1
		start = rf.LogRecord.startIndex()
	}

	for index := start; index <= rf.lastLogIndex(); index++ {
		if rf.LogRecord.term(index) != rf.currentTerm { // 5.4 (figure 8)
			continue
		}

		count := 1
		for i := 0; i < rf.numPeers; i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				count += 1
			}
		}
		// If majority, updates commitIndex
		if count > rf.numPeers/2 {
			Debug(dCommit, "S%d T%d, Commit || Updated commitIndex from %v to %v.\n", rf.me, rf.currentTerm, rf.commitIndex, index)
			rf.commitIndex = index
		}
	}
	rf.signalApplierL()
	rf.persist(nil)
}
