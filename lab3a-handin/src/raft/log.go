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
	if rf.LogRecord.len() == 1 {
		return rf.LastIncludedIndex
	}
	return rf.LogRecord.lastLogIndex()
}

func (rf *Raft) lastLogTerm() int {
	if rf.LogRecord.len() == 1 {
		return rf.LastIncludedTerm
	}
	Debug(dLog, "S%d T%d, || logs: %v.\n", rf.me, rf.currentTerm, rf.LogRecord)
	return rf.LogRecord.lastLogTerm()
}

// ***********************************
// ********** Logs Functions *********
// ***********************************

// Make a Logs struct with one 0 entry at Log
func mkLogEmpty(index0 int) LogRecord {
	return LogRecord{make([]Entry, 1), index0}
}

func mkLog(log []Entry, index0 int) LogRecord {
	newLog := append(make([]Entry, 1), log...)
	return LogRecord{newLog, index0}
}

func (l LogRecord) len() int {
	return len(l.Log)
}

// Convert from global log index to server LogRecord index.
func (l *LogRecord) toRecordIndex(logIndex int) int {
	return logIndex - l.Index0
}

// Convert from server LogRecord index to global log index.
func (l *LogRecord) toLogIndex(recordIndex int) int {
	return recordIndex + l.Index0
}

func (l LogRecord) firstOfTerm(term int, start int) int {
	resultIndex := start
	// indexExist && sameTerm
	for resultIndex > l.Index0 && l.term(resultIndex) == term {
		resultIndex -= 1
	}
	return resultIndex + 1
}

func (l LogRecord) lastOfTerm(term int, start int) int {
	resultIndex := start
	// indexExist && sameTerm
	for resultIndex > l.Index0 && l.term(resultIndex) == term {
		resultIndex -= 1
	}
	return resultIndex
}

func (l LogRecord) lastLogIndex() int {
	return l.toLogIndex(len(l.Log) - 1)
}

func (l LogRecord) lastLogTerm() int {
	return l.lastLogEntry().Term
}

func (l LogRecord) lastLogEntry() Entry {
	return l.Log[len(l.Log)-1]
}

func (l LogRecord) entry(logIndex int) Entry {
	recordIndex := l.toRecordIndex(logIndex)
	// Debug(dLog, "logIndex: %v, recordIndex:%v\n", logIndex, recordIndex)
	return l.Log[recordIndex]
}

func (l LogRecord) term(logIndex int) int {
	return l.entry(logIndex).Term
}

// slice Entry starting from the given logIndex index
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
