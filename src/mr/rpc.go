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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerArgs struct {
	MapNumber    int // finished maptask number
	ReduceNumber int // finished reducetask number
	Type         int // 0: map task, 1: reduce task
}

type WorkerReply struct {
	Type         int    // 0: map task, 1: reduce task, 2: waiting, 3: job finished
	NMap         int    // number of map task
	NReduce      int    // number of reduce task
	MapNumber    int    // map task only
	ReduceNumber int    // reducetask only
	Filename     string // maptask only
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
