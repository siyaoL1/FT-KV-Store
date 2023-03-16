package raft

type Entry struct {
	Command interface{}
	Term    int
}

// func (e Entry) String() string {
// 	return fmt.Sprintf("T %v", e.Term)
// }

type LogRecord struct {
	Log    []Entry
	Index0 int
}

// ***********************************
// ********** Logs Functions **********
// ***********************************

// Make a Logs struct with one 0 entry at Log
func mkLogEmpty() LogRecord {
	return LogRecord{make([]Entry, 1), 0}
}

func mkLog(Log []Entry, index0 int) LogRecord {
	return LogRecord{Log, index0}
}

func (l LogRecord) lastLogIndex() int {
	return len(l.Log) - 1
}

func (l LogRecord) lastLogTerm() int {
	return l.Log[len(l.Log)-1].Term
}

func (l LogRecord) lastLogEntry() Entry {
	return l.Log[len(l.Log)-1]
}

func (l LogRecord) entry(index int) Entry {
	return l.Log[index]
}

func (l LogRecord) slice(index int) []Entry {
	return l.Log[index:]
}

func (l LogRecord) start() int {
	return l.Index0
}

func (l *LogRecord) cutend(index int) {
	l.Log = l.Log[:index]
}

func (l *LogRecord) append(entry Entry) {
	l.Log = append(l.Log, entry)
}
