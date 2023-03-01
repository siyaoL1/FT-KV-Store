package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := RequestTask()

		switch reply.Type {
		case 0:
			// fmt.Println("This is map task")
			//
			// read each input file,
			// pass it to Map,
			// accumulate the intermediate Map output.
			//
			intermediate := []KeyValue{}

			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			intermediate = append(intermediate, kva...)

			// create buckets
			buckets := make([][]KeyValue, reply.NReduce)
			// initialize each bucket
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			// put intermediate into buckets based on hash
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%reply.NReduce] = append(buckets[ihash(kva.Key)%reply.NReduce], kva)
			}
			// write to json file
			for i := range buckets {
				newfilename := fmt.Sprintf("mr-%d-%d*", reply.MapNumber, i)
				newfile, _ := ioutil.TempFile("", newfilename)
				enc := json.NewEncoder(newfile)
				for _, kv := range buckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Println("encoding error...")
					}
				}
				os.Rename(newfile.Name(), fmt.Sprintf("mr-%d-%d", reply.MapNumber, i))
				newfile.Close()
			}
			// finish a map task
			newargs := WorkerArgs{reply.MapNumber, -1, 0}
			newreply := WorkerReply{}
			call("Coordinator.ReceiveFinishedMap", &newargs, &newreply)
		case 1:
			// fmt.Println("This is a reduce task")
			kva := []KeyValue{}
			// Open Reduce task files
			for i := 0; i < reply.NMap; i++ {
				rfilename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceNumber)
				rfile, err := os.Open(rfilename)
				if err != nil {
					log.Fatalf("cannot open %v", rfilename)
				}
				dec := json.NewDecoder(rfile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						// fmt.Print(err)
						break
					}
					kva = append(kva, kv)
				}
				rfile.Close()
			}
			sort.Sort(ByKey(kva))
			// fmt.Printf("kva len after sorting: %d\n", len(kva))
			rname := fmt.Sprintf("mr-out-%d*", reply.ReduceNumber)
			rfile, _ := ioutil.TempFile("", rname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-ReduceNumber.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(rfile, "%v %v\n", kva[i].Key, output)
				// fmt.Printf("%v %v\n", kva[i].Key, output)
				i = j
			}
			os.Rename(rfile.Name(), fmt.Sprintf("mr-out-%d", reply.ReduceNumber))
			rfile.Close()
			// remove intermediate files
			for i := 0; i < reply.NMap; i++ {
				rfilename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceNumber)
				err := os.Remove(rfilename)
				if err != nil {
					fmt.Printf("cannot remove %v\n", rfilename)
				}
			}
			// finish a reduce task
			newargs := WorkerArgs{-1, reply.ReduceNumber, 1}
			newreply := WorkerReply{}
			call("Coordinator.ReceiveFinishedReduce", &newargs, &newreply)
			// case 2: // idle
			// 	break
		}
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Request new task for worker
func RequestTask() WorkerReply {
	// declare an argument structure.
	args := WorkerArgs{}

	// declare a reply structure.
	reply := WorkerReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.Allocate", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
