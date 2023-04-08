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
	for {
		// declare an argument structure.
		args := WorkerArgs{}
		args.Status = IDLE
		// declare a reply structure.
		reply := CoordReply{}
		// make call to corrdinator
		WorkerCall(&args, &reply)

		for reply.Task == MAP {
			args.Status = MAP_DONE
			args.Payload.MapFile = reply.Payload.MapFile
			args.Payload.IntermediateFilePrefix = mapTask(mapf, reply.Payload.MapFile, reply.Payload.NumReduce)
			DPrintf("intermediateFilePrefix %v\n", args.Payload.IntermediateFilePrefix)
			WorkerCall(&args, &reply)
		}

		for reply.Task == REDUCE {
			args.Status = REDUCE_DONE
			args.Payload.ReduceNumber = reply.Payload.ReduceNumber
			DPrintf("Reduce number is %d\n", reply.Payload.ReduceNumber)
			reduceTask(reducef, reply.Payload.IntermediateFilePrefixList, reply.Payload.ReduceNumber)
			WorkerCall(&args, &reply)
		}

		if reply.Task == WAIT {
			time.Sleep(1 * time.Second)
		}

		if reply.Task == DONE {
			return
		}
	}
}

// Map function
func mapTask(mapf func(string, string) []KeyValue, filename string, numReduce int) string {
	// Load the data from file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Process into kv pairs and add it to intermediate slice
	kva := mapf(filename, string(content))
	intermediate := []KeyValue{} // an KeyValue slice
	intermediate = append(intermediate, kva...)

	// Create intermediate files
	var intermediateFiles []*os.File
	var intermediateEncoders []*json.Encoder
	for i := 0; i < numReduce; i++ {
		file, err = ioutil.TempFile("", fmt.Sprint("mr-", filename[3:], "-", i))
		if err != nil {
			log.Fatal(err)
			log.Fatalf("cannot create tempfile")
		}
		intermediateFiles = append(intermediateFiles, file)
		intermediateEncoders = append(intermediateEncoders, json.NewEncoder(file))
	}

	// Partition intermediate data into intermediate files
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % numReduce
		err := intermediateEncoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write encoded kv to file")
		}
	}

	// Rename the intermediate files
	for i := 0; i < numReduce; i++ {
		tempFilename := intermediateFiles[i].Name()
		finalFilename := fmt.Sprint("mr-", filename[3:], "-", i)
		DPrintf("%v, %s\n", i, finalFilename)
		intermediateFiles[i].Close()
		os.Rename(tempFilename, finalFilename)
	}

	return fmt.Sprint("mr-", filename[3:], "-")
}

func reduceTask(reducef func(string, []string) string, prefixList []string, reduceNumber int) {
	// Create temp output file
	oname := fmt.Sprint("mr-out-", reduceNumber)
	ofile, err := ioutil.TempFile(".", oname)
	if err != nil {
		log.Fatalf("cannot create tempfile")
	}

	// Load intermediate results
	intermediate := []KeyValue{}

	for _, prefix := range prefixList {
		intermediateFile, err := os.Open(fmt.Sprint(prefix, reduceNumber-1))
		if err != nil {
			log.Fatalf("cannot open intermediate file: %s\n", fmt.Sprint(prefix, reduceNumber))
		}
		// Decode data
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		intermediateFile.Close()
	}
	// Sort intermediate data
	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-xxx.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFilename := ofile.Name()
	ofile.Close()
	os.Rename(tempFilename, oname)

	DPrintf("Finished Reduce task for number: %d\n", reduceNumber)
}

func WorkerCall(args *WorkerArgs, reply *CoordReply) {
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.Message", args, reply)
	DPrintf("Sending status %v to coord\n", args.Status)

	if ok {
		DPrintf("Received %v task\n", reply.Task)
	} else {
		DPrintf("call failed!\n")
	}
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
