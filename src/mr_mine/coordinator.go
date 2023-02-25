package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Contains shared mutex, map task data, reduce task data
	RPCMutex                   sync.Mutex
	NumReduce                  int
	MapFiles                   []string
	MapTasks                   map[string]time.Time
	MapDone                    bool
	IntermediateFilePrefixList []string
	ReduceCount                int
	ReduceTasks                map[int]time.Time // {reduce#, time}
	ReduceDone                 bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Message(args *WorkerArgs, reply *CoordReply) error {
	if args.Status == MAP_DONE {
		processMapResult(c, args)
	} else if args.Status == REDUCE_DONE {
		processReduceResult(c, args)
	}

	c.RPCMutex.Lock()
	if !c.MapDone {
		c.RPCMutex.Unlock()
		sendMapTask(c, reply)
	} else if !c.ReduceDone {
		c.RPCMutex.Unlock()
		sendReduceTask(c, reply)
	} else {
		c.RPCMutex.Unlock()
		sendDone(c, reply)
	}
	return nil
}

func sendMapTask(c *Coordinator, reply *CoordReply) {
	if os.Getenv("LOG") == "1" {
		fmt.Printf("In sendMapTask function\n")
	}

	c.RPCMutex.Lock()
	defer c.RPCMutex.Unlock()

	// There's still map jobs that are not assigned yet
	if len(c.MapFiles) > 0 {
		// Remove the last file in the slice
		mapFile := c.MapFiles[len(c.MapFiles)-1]
		c.MapFiles = c.MapFiles[:len(c.MapFiles)-1]
		// Add the mapfile to the working array and mark the time stamp
		c.MapTasks[mapFile] = time.Now()

		reply.Task = MAP
		reply.Payload.MapFile = mapFile
		reply.Payload.NumReduce = c.NumReduce

		// There's on going tasks that not yet finished
	} else if !c.MapDone {
		for mapFile, startTime := range c.MapTasks {
			if time.Since(startTime).Seconds() > 10 {
				c.MapTasks[mapFile] = time.Now()
				reply.Task = MAP
				reply.Payload.MapFile = mapFile
				reply.Payload.NumReduce = c.NumReduce
				return
			} else {
				reply.Task = WAIT
			}
		}
	}
}

func sendReduceTask(c *Coordinator, reply *CoordReply) {
	if os.Getenv("LOG") == "1" {
		fmt.Printf("In send Reduce Task function\n")
	}

	c.RPCMutex.Lock()
	defer c.RPCMutex.Unlock()

	// There's still reduce jobs that are not assigned yet
	fmt.Printf("Reduce Count is: %d", c.ReduceCount)
	if c.ReduceCount > 0 {
		// Decrement reduce count
		reduceNumber := c.ReduceCount
		c.ReduceCount -= 1
		fmt.Printf("reduceNumber: %d\n", reduceNumber)
		// Mark the time stamp for the assigned Reduce ID
		c.ReduceTasks[reduceNumber] = time.Now()

		reply.Task = REDUCE
		reply.Payload.ReduceNumber = reduceNumber
		reply.Payload.IntermediateFilePrefixList = c.IntermediateFilePrefixList

	} else if !c.ReduceDone {
		for reduceNumber, startTime := range c.ReduceTasks {
			if time.Since(startTime).Seconds() > 10 {
				c.ReduceTasks[reduceNumber] = time.Now()
				reply.Task = REDUCE
				reply.Payload.ReduceNumber = reduceNumber
				reply.Payload.IntermediateFilePrefixList = c.IntermediateFilePrefixList
				return
			} else {
				reply.Task = WAIT
			}
		}
	}
}

func sendDone(c *Coordinator, reply *CoordReply) {
	if os.Getenv("LOG") == "1" {
		fmt.Printf("In sendDone function\n")
	}
	c.RPCMutex.Lock()
	reply.Task = DONE
	c.RPCMutex.Unlock()
}

// Store the intermediate file path into the reduce tasks array
func processMapResult(c *Coordinator, args *WorkerArgs) {
	if os.Getenv("LOG") == "1" {
		fmt.Printf("Processing map results\n")
	}

	c.RPCMutex.Lock()
	defer c.RPCMutex.Unlock()

	if _, ok := c.MapTasks[args.Payload.MapFile]; ok {
		// Remove corresponding map task
		delete(c.MapTasks, args.Payload.MapFile)
		// Save intermediate result prefix
		c.IntermediateFilePrefixList = append(c.IntermediateFilePrefixList, args.Payload.IntermediateFilePrefix)
		// Check if map are all finished
		if len(c.MapFiles) == 0 && len(c.MapTasks) == 0 {
			c.MapDone = true
		}
	}
}

// Process reduce result
func processReduceResult(c *Coordinator, args *WorkerArgs) {
	if os.Getenv("LOG") == "1" {
		fmt.Printf("Processing reduce results\n")
	}

	c.RPCMutex.Lock()
	defer c.RPCMutex.Unlock()

	if _, ok := c.ReduceTasks[args.Payload.ReduceNumber]; ok {
		// Remove corresponding reduce task
		delete(c.ReduceTasks, args.Payload.ReduceNumber)
		// Remove the intermediate files
		for _, prefix := range c.IntermediateFilePrefixList {
			os.Remove(fmt.Sprint(prefix, args.Payload.ReduceNumber-1))
		}
		// Check if reduce are all finished
		if c.ReduceCount == 0 && len(c.ReduceTasks) == 0 {
			c.ReduceDone = true
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.ReduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Enable logging //
	os.Setenv("LOG", "1")
	// Init map file array
	var mapFiles []string
	mapFiles = append(mapFiles, files...)
	// Init intermediate file array

	c := Coordinator{
		RPCMutex:                   sync.Mutex{},
		NumReduce:                  nReduce,
		MapFiles:                   mapFiles,
		MapTasks:                   map[string]time.Time{},
		MapDone:                    false,
		IntermediateFilePrefixList: []string{},
		ReduceCount:                nReduce,
		ReduceTasks:                map[int]time.Time{},
		ReduceDone:                 false,
	}

	c.server()
	return &c
}
