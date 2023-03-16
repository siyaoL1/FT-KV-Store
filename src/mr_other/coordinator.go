package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int        // number of total reduce tasks
	nMap           int        // number of total map tasks
	reducefinished int        // number of reduce task done
	mapfinished    int        // number of map task done
	mu             sync.Mutex // lock
	files          []string
	reducetype     []int // 0 for not yet started task; 1 for task that is being executed; 2 for task done
	maptype        []int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Allocate new task to worker
func (c *Coordinator) Allocate(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	if c.mapfinished < c.nMap {
		cnt := -1
		for i, status := range c.maptype {
			if status == 0 {
				cnt = i
				break
			}
		}
		if cnt < 0 {
			reply.Type = 2
			c.mu.Unlock()
			return nil
		}
		reply.Filename = c.files[cnt]
		reply.MapNumber = cnt
		reply.Type = 0
		c.maptype[cnt] = 1
		c.mu.Unlock()
		go func() {
			time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
			c.mu.Lock()
			if c.maptype[cnt] == 1 {
				c.maptype[cnt] = 0
			}
			c.mu.Unlock()
		}()
	} else if c.mapfinished == c.nMap && c.reducefinished < c.nReduce {
		cnt := -1
		for i, status := range c.reducetype {
			if status == 0 {
				cnt = i
				break
			}
		}
		if cnt < 0 {
			reply.Type = 2
			c.mu.Unlock()
			return nil
		}
		reply.ReduceNumber = cnt
		reply.Type = 1
		c.reducetype[cnt] = 1
		c.mu.Unlock()
		go func() {
			time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
			c.mu.Lock()
			if c.reducetype[cnt] == 1 {
				c.reducetype[cnt] = 0
			}
			c.mu.Unlock()
		}()
	} else {
		reply.Type = 2
		c.mu.Unlock()
	}

	return nil
}

func (c *Coordinator) ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	c.mapfinished++
	c.maptype[args.MapNumber] = 2 // log the map task as finished
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	c.reducefinished++
	c.reducetype[args.ReduceNumber] = 2 // log the reduce task as finished
	c.mu.Unlock()
	return nil
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
	ret := false

	// Your code here.
	ret = c.reducefinished == c.nReduce

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files) // 8 for now
	c.nReduce = nReduce
	c.files = files
	c.maptype = make([]int, c.nMap)
	c.reducetype = make([]int, nReduce)

	c.server()
	return &c
}
