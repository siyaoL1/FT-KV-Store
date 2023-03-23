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
// ********** Raft Functions *********
// ***********************************
func (rf *Raft) lastLogIndex() int {
	// if rf.LogRecord.len() == 0 {
	// 	return rf.LastIncludedIndex
	// }
	return rf.LogRecord.lastLogIndex()
}

func (rf *Raft) lastLogTerm() int {
	// if rf.LogRecord.len() == 0 {
	// 	return rf.LastIncludedTerm
	// }
	return rf.LogRecord.lastLogTerm()
}

// ***********************************
// ********** Logs Functions *********
// ***********************************

// Make a Logs struct with one 0 entry at Log
func mkLogEmpty() LogRecord {
	return LogRecord{make([]Entry, 1), 0}
}

func mkLog(Log []Entry, index0 int) LogRecord {
	return LogRecord{Log, index0}
}

func (l LogRecord) len() int {
	return len(l.Log)
}

func (l LogRecord) lastLogIndex() int {
	return l.toLogIndex(len(l.Log) - 1)
}

func (l LogRecord) lastLogTerm() int {
	return l.Log[len(l.Log)-1].Term
}

func (l LogRecord) lastLogEntry() Entry {
	return l.Log[len(l.Log)-1]
}

func (l LogRecord) entry(logIndex int) Entry {
	recordIndex := l.toRecordIndex(logIndex)
	Debug(dLog, "logIndex: %v, recordIndex:%v\n", logIndex, recordIndex)
	return l.Log[recordIndex]
}

func (l LogRecord) slice(logIndex int) []Entry {
	recordIndex := l.toRecordIndex(logIndex)
	return l.Log[recordIndex:]
}

func (l LogRecord) startIndex() int {
	return l.Index0
}

func (l *LogRecord) cutend(logIndex int) {
	recordIndex := l.toRecordIndex(logIndex)
	l.Log = l.Log[:recordIndex]
}

func (l *LogRecord) append(entry Entry) {
	l.Log = append(l.Log, entry)
}

func (l *LogRecord) toRecordIndex(logIndex int) int {
	return logIndex - l.Index0
}

func (l *LogRecord) toLogIndex(recordIndex int) int {
	return recordIndex + l.Index0
}
