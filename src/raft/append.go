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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictFirst int
	ConflictValid bool
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

		// Construct Args for AppendEntries RPC
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.LogRecord.entry(rf.nextIndex[server] - 1).Term,
			LeaderCommit: rf.commitIndex,
		}

		// When there's logs to replicate
		if rf.LogRecord.lastLogIndex() >= rf.nextIndex[server] {
			args.Entries = make([]Entry, rf.LogRecord.lastLogIndex()-rf.nextIndex[server]+1)
			// starting from the nextIndex which is prevIndex + 1
			copy(args.Entries, rf.LogRecord.slice(rf.nextIndex[server]))
		}

		DPrintf("(Leader %v, term: %v) || Send || Replicating logs to %v, log length: %v, PrevLogIndex: %v, PrevLogTerm:%v, LeaderCommit: %v \n", rf.me, rf.currentTerm, server, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

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
	// DPrintf("(Server %v, term: %v) sending AppendEntries RPC to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		DPrintf("Failed in AppendEntries PRC from %v to %v! (term: %v)\n", rf.me, server, args.Term)
	}
	return ok
}

// ***************************************
// ******** AppendEntries Handler ********
// ***************************************

// AppendEntries RPC request handler (being handled concurrently)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("(Server %v, term: %v) || Receive || Received AppendEntry RPC ||\n", rf.me, rf.currentTerm)
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
	// First two cases are Rule 2
	if args.PrevLogIndex > rf.LogRecord.lastLogIndex() || args.PrevLogIndex < rf.LogRecord.start() {
		// invalid prev log index
		// Tell leader to backup nextIndex[]
		reply.ConflictTerm = args.Term + 1
		reply.ConflictFirst = rf.LogRecord.lastLogIndex()
		reply.ConflictValid = true
	} else if rf.LogRecord.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// conflict entry, need to tell leader to roll back
		reply.ConflictTerm = rf.LogRecord.lastLogEntry().Term
		reply.ConflictFirst = rf.LogRecord.lastLogIndex()
		reply.ConflictValid = true
		// Roll back the entire term
		i := rf.LogRecord.lastLogIndex()
		for i > rf.LogRecord.start()+1 && rf.LogRecord.entry(i).Term == reply.ConflictTerm {
			reply.ConflictFirst = i
			i -= 1
		}
	} else {
		rf.updateLogL(args, reply)
		reply.Success = true
	}

	// DPrintf("(Server %v, term: %v) || Current log: %v.||\n", rf.me, rf.currentTerm, rf.log)
	rf.leaderID = args.LeaderID

	rf.signalApplierL()
	reply.Term = rf.currentTerm
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
		if index <= rf.LogRecord.lastLogIndex() && rf.LogRecord.entry(index).Term != entry.Term {
			(&(rf.LogRecord)).cutend(index)
		}
	}

	// Rule 4
	for i, entry := range args.Entries {
		// The index that new entry will be appended at
		index := args.PrevLogIndex + 1 + i
		if index == rf.LogRecord.lastLogIndex()+1 {
			rf.LogRecord.append(entry)
			DPrintf("(Server %v, term: %v) || Append || Appended 1 log entry, lastLogIndex: %v.\n", rf.me, rf.currentTerm, rf.LogRecord.lastLogIndex())
		}
		if !reflect.DeepEqual(rf.LogRecord.entry(index).Command, entry.Command) {
			DPrintf("Entry error")
			log.Fatalf("Entry error %v from=%v index%v old=%v new=%v\n",
				rf.me, args.LeaderID, index, rf.LogRecord.entry(index), args.Entries[i])
		}
	}

	// Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		latestIndex := args.PrevLogIndex + 1 + len(args.Entries) - 1
		DPrintf("(Server %v, term: %v) || Commit || Commit to %v\n", rf.me, rf.currentTerm, rf.commitIndex)
		if rf.commitIndex > latestIndex {
			DPrintf("(Server %v, term: %v) || Commit || Commit to %v\n", rf.me, rf.currentTerm, latestIndex)
			rf.commitIndex = latestIndex
		}
	}

	// Write log to stable storage
	rf.persist()
}

// ***************************************
// ********   Response Handler   *********
// ***************************************

// AppendEntries RPC response Handler
func (rf *Raft) processAppendReplyL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Handle AppendEntries result
	DPrintf("(Leader %v, term: %v) || Response || Received AppendEntries response from %v with success: %v \n", rf.me, rf.currentTerm, server, reply.Success)
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
				DPrintf("(Leader %v, term: %v) || Update || Updated server %v with nextIndex: %v, matchIndex: %v \n", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
			}
		} else if reply.ConflictValid { // Failure
			// Backup one term
			rf.conflictTermL(server, args, reply)
		} else if rf.nextIndex[server] > 1 {
			// Backup one index
			DPrintf("(Leader %v, term: %v) || Update || Decremented server %v's nextIndex to %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server])
			rf.nextIndex[server] -= 1
			// 2D
			// if rf.nextIndex[peer] < rf.log.start() + 1 {
			// 	rf.sendSnaphsot(peer)
			// }
		}
		rf.advancedCommitL()
		// DPrintf("(Server %v, term: %v) received AppendEntries reply from %v (term %v)\n", rf.me, reply.Term, server)

	}
}

func (rf *Raft) conflictTermL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	hasTerm := false
	firstTermIndex := 0

	for i, entry := range rf.LogRecord.Log {
		if entry.Term == reply.Term {
			hasTerm = true
			firstTermIndex = len(rf.LogRecord.Log) - i - 1
			break
		} else if entry.Term < reply.Term {
			break
		}
	}

	if !hasTerm {
		DPrintf("(Leader %v, term: %v) || Update || 1 Decremented server %v's nextIndex from %v to %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server], reply.ConflictFirst)
		rf.nextIndex[server] = reply.ConflictFirst
		// TODO: stopped here, need to check which condition caused the conflictfirst to be higher than actual index.
		// if reply.ConflictFirst < rf.nextIndex[server] {
		// 	rf.nextIndex[server] = reply.ConflictFirst
		// }
	} else {
		lastTermIndex := firstTermIndex
		for lastTermIndex <= rf.LogRecord.lastLogIndex() &&
			rf.LogRecord.entry(firstTermIndex).Term == rf.LogRecord.entry(lastTermIndex).Term {
			lastTermIndex += 1
		}
		DPrintf("(Leader %v, term: %v) || Update || 2 Decremented server %v's nextIndex from %v to %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server], lastTermIndex-1)
		rf.nextIndex[server] = lastTermIndex - 1
		// if rf.nextIndex[server] > repl
	}
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
	if start < rf.LogRecord.start() { // 2D: when restart, start could be 1
		start = rf.LogRecord.start()
	}

	for index := start; index <= rf.LogRecord.lastLogIndex(); index++ {
		if rf.LogRecord.entry(index).Term != rf.currentTerm { // 5.4 (figure 8)
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
			DPrintf("(Leader %v, term: %v) || Updated || Updated commitIndex from %v to %v.||\n", rf.me, rf.currentTerm, rf.commitIndex, index)
			rf.commitIndex = index
		}
	}
	rf.signalApplierL()
	rf.persist()
}
