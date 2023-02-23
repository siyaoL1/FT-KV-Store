package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Type Definition
type status int64
type task int
type payload struct {
	MapFile                    string
	IntermediateFilePrefix     string
	IntermediateFilePrefixList []string
	ReduceFilePrefix           string
	ReduceNumber               int
	NumReduce                  int
}

const (
	IDLE        status = 0
	IN_PROGRESS status = 1
	MAP_DONE    status = 2
	REDUCE_DONE status = 3
)

const (
	MAP    task = 0
	REDUCE task = 1
	WAIT   task = 2
	DONE   task = 3
)

// RPC definitions.
type WorkerArgs struct {
	Status  status
	Payload payload
}

type CoordReply struct {
	Task    task
	Payload payload
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
