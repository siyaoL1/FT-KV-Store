package raft

import "fmt"

type Entry struct {
	Command interface{}
	Term    int
}

func (e Entry) String() string {
	return fmt.Sprintf("T %v", e.Term)
}

type Log struct {
	log    []Entry
	index0 int
}

// Make a Log struct with one 0 entry at log
func mkLogEmpty() Log {
	return Log{make([]Entry, 1), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l Log) lastLogIndex() int {
	return len(l.log) - 1
}

func (l Log) lastLogTerm() int {
	return l.log[len(l.log)-1].Term
}

func (l Log) lastLogEntry() Entry {
	return l.log[len(l.log)-1]
}

func (l Log) entry(index int) Entry {
	return l.log[index]
}

func (l Log) slice(index int) []Entry {
	return l.log[index:]
}

func (l Log) start() int {
	return l.index0
}

func (l Log) cutend(index int) {
	l.log = l.log[:index]
}

func (l Log) append(entry Entry) {
	l.log = append(l.log, entry)
}
