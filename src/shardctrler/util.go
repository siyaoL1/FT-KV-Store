package shardctrler

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

type logTopic string

const (
	// ShardCtrler
	dScJoin    logTopic = "JOIN"
	dScLeave   logTopic = "LEAV"
	dScMove    logTopic = "MOVE"
	dScQuery   logTopic = "QUER"
	dScGeneral logTopic = "CONF"
	dScApplier logTopic = "SAPL"
	dScApQuery logTopic = "SAQY"
	dScApJoin  logTopic = "SAJN"
	dScApLeave logTopic = "SALE"
	dScApMove  logTopic = "SAMV"
	dScClient  logTopic = "CLNT"
	// KvRaft
	dClerk     logTopic = "CLRK"
	dKvGet     logTopic = "KGET"
	dKvPutApp  logTopic = "KP&A"
	dKvApplier logTopic = "KAPL"
	dKvApGet   logTopic = "KAPG"
	dKvApPut   logTopic = "KAPP"
	dKvApApd   logTopic = "KAPA"
	dKvSnap    logTopic = "KSNP"
	// Raft
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dVote    logTopic = "VOTE"
	// General
	dTest  logTopic = "TEST"
	dTimer logTopic = "TIMR"
	dTrace logTopic = "TRCE"
	dWarn  logTopic = "WARN"
	dError logTopic = "ERRO"
	dInfo  logTopic = "INFO"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
