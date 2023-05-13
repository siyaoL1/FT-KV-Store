package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"6.5840/raft"
	"6.5840/shardctrler"
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
	// KV
	dKvConfig        logTopic = "KCFG"
	dKvRequestShard  logTopic = "KSND"
	dKvRecvShard     logTopic = "KRCV"
	dKvApConfig      logTopic = "KAPC"
	dKvUpdateShard   logTopic = "KUPD"
	dKvApUpdateShard logTopic = "KAUP"
	dKvGet           logTopic = "KGET"
	dKvPutApp        logTopic = "KP&A"
	dKvApplier       logTopic = "KAPL"
	dKvApGet         logTopic = "KAPG"
	dKvApApd         logTopic = "KAPA"
	dKvSnap          logTopic = "KSNP"
	dClerk           logTopic = "CLRK"

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
	// dKvApPut logTopic = "KAPP"
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

// return the string representation of the data based on the type
func String(data interface{}) string {
	switch value := data.(type) {
	case map[string]string:
		dict := value
		result := ""
		result += "{"
		for key, value := range dict {
			result += fmt.Sprintf("%v: %v, ", key, value)
		}
		result += "}"
		return result
	case map[int][]string:
		dict := value
		result := ""
		result += "{"
		for key, value := range dict {
			result += fmt.Sprintf("%v: %+v, ", key, value)
		}
		return result
	case shardctrler.Config:
		config := value
		result := ""
		result += fmt.Sprintf("{ConfigNum: %v ", config.Num)
		result += fmt.Sprintf(", Shards: %+v", config.Shards)
		result += fmt.Sprintf(", Groups: %+v}", config.Groups)
		return result
	case []bool:
		arr := value
		strArr := make([]string, len(arr))
		for i, v := range arr {
			strArr[i] = strconv.FormatBool(v)
		}
		return fmt.Sprintf("{%v}", strings.Join(strArr, ", "))
	case raft.ApplyMsg:
		result := ""
		msg := value
		if msg.CommandValid {
			op := msg.Command.(Op)
			if op.OpType == "Put" || op.OpType == "Append" || op.OpType == "Get" {
				result += fmt.Sprintf("{ OpType: %v, Key: %v, Value: %v, OpNum: %v, Client: %v }", op.OpType, op.Key, op.Value, op.OpNum, op.Client)
			} else if op.OpType == "Config" {
				result += fmt.Sprintf("{ OpType: %v, Config: %v }", op.OpType, String(op.Config))
			} else if op.OpType == "UpdateShard" {
				result += fmt.Sprintf("{ OpType: %v, Shard: %v, Config: %v }", op.OpType, op.ShardNum, String(op.Config))
			} else {
				result += fmt.Sprintf("%+v\n", msg)
			}
		} else {
			result += fmt.Sprintf("{ Snapshot: %v, SnapshotIndex: %v, SnapshotTerm: %v }", msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
		}
		return result
	case Result:
		result := ""
		res := value
		if res.Op.OpType == "Get" || res.Op.OpType == "Put" || res.Op.OpType == "Append" {
			result += fmt.Sprintf("{ Err: %+v, Key: %v, Value: %v, OpNum: %v }", res.Err, res.Op.Key, res.Value, res.Op.OpNum)
		} else {
			result += fmt.Sprintf("{ Op: %v, Err: %v, Config: %v }", res.Op, res.Err, String(res.Op.Config))
		}
		return result
	default:
		return ""
	}
}
