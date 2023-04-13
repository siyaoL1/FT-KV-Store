package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	OpNum  int
	Client int64
}

type Result struct {
	Err   Err
	Value string
	Op    Op
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	indexToCh   map[int]chan Result
	dataDict    map[string]string
	lastCommand map[int64]int // stores OpNum of the last
	// lastApplied int           // last applied index
}

// ***************************************
// ************  Get Handler  ************
// ***************************************

// Get RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	if kv.killed() {
		return
	}

	// Construct Operation and send to Raft
	op := Op{
		OpType: "Get",
		Key:    args.Key,
		OpNum:  args.OpNum,
		Client: args.Client,
	}
	index, _, isLeader := kv.rf.Start(op)

	// The Raft server is not leader
	if !isLeader {
		*reply = GetReply{Err: ErrWrongLeader}
		return
	}
	Debug(dKvGet, "KV%v | Get | Receieved start result from raft, index: %v\n", kv.me, index)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	kv.mu.Unlock()

	Debug(dKvGet, "KV%v | Get | Waiting for message from resultCh: %+v\n", kv.me, resultCh)
	var result Result
	var ok bool
	select {
	case result, ok = <-resultCh:
		if !ok || result.Err == ErrWrongLeader || result.Op != op {
			*reply = GetReply{
				Err: ErrWrongLeader,
			}
		} else {
			*reply = GetReply{
				Err:   result.Err,
				Value: result.Value,
			}
		}
	case <-time.After(1000 * time.Millisecond):
		*reply = GetReply{Err: ErrWrongLeader}
	}

	// Delete the used index
	kv.mu.Lock()
	delete(kv.indexToCh, index)
	kv.mu.Unlock()
	Debug(dKvGet, "KV%v | Get | Sending result to Clerk, resultCh: %v, index: %v, result: %+v\n", kv.me, resultCh, index, result)
}

// ***************************************
// ********  Put/Append Handler  *********
// ***************************************

// PutAppend RPC handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Debug(,"KV%v | PutAppend | Received RPC with args: %+v\n", kv.me, args)
	// Your code here.

	if kv.killed() {
		return
	}

	// Construct Operation and send to Raft
	op := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		OpNum:  args.OpNum,
		Client: args.Client,
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()
	// The Raft server is not leader
	if !isLeader {
		*reply = PutAppendReply{Err: ErrWrongLeader}
		return
	}

	Debug(dKvPutApp, "KV%v | PutAppend | Receieved start result from raft leader, index: %v\n", kv.me, index)
	// Debug(dKvPutApp, "KV%v | PutAppend | Raft: %+v,\n", kv.me, kv.rf)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	Debug(dKvPutApp, "KV%v | resultCh | indexToCh:%+v, index:%v\n", kv.me, kv.indexToCh, index)
	kv.mu.Unlock()

	Debug(dKvPutApp, "KV%v | PutAppend | Start waiting for resultCh message\n", kv.me)
	var result Result
	var ok bool
	select {
	case result, ok = <-resultCh:
		Debug(dKvPutApp, "KV%v | PutAppend | Received message from resultCh: %+v\n", kv.me, result)
		// if result.
		if !ok || result.Err == ErrWrongLeader || result.Op != op {
			*reply = PutAppendReply{
				Err: ErrWrongLeader,
			}
		} else {
			*reply = PutAppendReply{
				Err: result.Err,
			}
		}
	case <-time.After(1000 * time.Millisecond):
		*reply = PutAppendReply{
			Err: ErrWrongLeader,
		}
	}
	// Delete the used index
	Debug(dKvPutApp, "KV%v | PutAppend | Sending result to Clerk, resultCh: %v, index: %v, result: %+v\n", kv.me, resultCh, index, result)
	kv.mu.Lock()
	delete(kv.indexToCh, index)
	kv.mu.Unlock()
}

// ***************************************
// ************    Helper   **************
// ***************************************

func (kv *KVServer) checkDuplicateL(client int64, opNum int) bool {
	lastOpNum, ok := kv.lastCommand[client]
	return ok && lastOpNum >= opNum
}

// ***************************************
// ************ CommandValid *************
// ***************************************

func (kv *KVServer) processCommandValid(m raft.ApplyMsg) Result {
	op := m.Command.(Op)
	var result Result

	kv.mu.Lock()
	// kv.lastApplied = m.CommandIndex
	switch op.OpType {
	case "Get":
		value, ok := kv.dataDict[op.Key]
		if ok { // key exist
			result = Result{
				Err:   OK,
				Value: value,
				Op:    op,
			}
		} else { // key doesn't exist
			result = Result{
				Err:   ErrNoKey,
				Value: "",
				Op:    op,
			}
		}
		Debug(dKvApGet, "KV%v | Apply Get | Applied Get: %+v, dataDict: %+v\n", kv.me, result, kv.dataDict)

	case "Put":
		if !kv.checkDuplicateL(op.Client, op.OpNum) {
			if kv.lastCommand == nil {
				fmt.Printf("KV%v | lastCommand is nil\n", kv.me)
			}
			kv.lastCommand[op.Client] = op.OpNum
			kv.dataDict[op.Key] = op.Value
		}
		result = Result{
			Err: OK,
			Op:  op,
		}
		Debug(dKvApPut, "KV%v | Apply Put | Applied Put: %+v, dataDict: %+v\n", kv.me, result, kv.dataDict)

	case "Append":
		if !kv.checkDuplicateL(op.Client, op.OpNum) {
			kv.lastCommand[op.Client] = op.OpNum
			kv.dataDict[op.Key] += op.Value
		}
		result = Result{
			Err: OK,
			Op:  op,
		}
		Debug(dKvApApd, "KV%v | Apply Append | Applied Append: %+v, dataDict: %+v\n", kv.me, result, kv.dataDict)

	}
	kv.mu.Unlock()

	return result
}

// ***************************************
// ************    Snapshot  *************
// ***************************************
func (kv *KVServer) checkSnapshot(index int) {
	curr_size := kv.rf.GetStateSize()
	if kv.maxraftstate != -1 && curr_size > kv.maxraftstate/2 {
		Debug(dKvSnap, "KV%v | Snapshot | Raft state size: %v, maxraftstate: %v\n", kv.me, curr_size, kv.maxraftstate)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.dataDict)
		e.Encode(kv.lastCommand)
		// e.Encode(kv.lastApplied)
		data := w.Bytes()
		go kv.rf.Snapshot(index, data)
		Debug(dKvSnap, "KV%v | Snapshot | Snapshoted, current Raft state size: %v\n", kv.me, index, data, kv.rf.GetStateSize())
	}
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	Debug(dKvSnap, "KV%v | Snapshot | Install snapshot, snapshot: %v\n", kv.me, snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.dataDict)
	d.Decode(&kv.lastCommand)
	// d.Decode(&kv.lastApplied)
}

// func (kv *KVServer) snapshotter() {
// 	for {
// 		kv.checkSnapshot(kv.lastApplied)
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// ***************************************
// ************     Apply    *************
// ***************************************

func (kv *KVServer) apply(index int, result Result) {
	kv.mu.Lock()
	resultCh, ok := kv.indexToCh[index]
	kv.mu.Unlock()

	Debug(dKvApplier, "KV%v | resultCh | resultCh:%v, ok:%v\n", kv.me, resultCh, ok)
	if ok {
		resultCh <- result
		Debug(dKvApplier, "KV%v | applier | Sent result to resultCh: %+v\n", kv.me, resultCh)
	}
}

func (kv *KVServer) applier() {
	for {
		Debug(dKvApplier, "KV%v | applier | Waiting for message on applych: %v\n", kv.me, kv.applyCh)
		m := <-kv.applyCh
		Debug(dKvApplier, "KV%v | applier | Received message from applyCh: %+v\n", kv.me, m)

		if m.SnapshotValid {
			kv.installSnapshot(m.Snapshot)
		} else if m.CommandValid {
			result := kv.processCommandValid(m)
			kv.apply(m.CommandIndex, result)
			go kv.checkSnapshot(m.CommandIndex)
		} else {
			continue
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// You may need initialization code here.
	kv := new(KVServer)
	kv.mu = sync.Mutex{}
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0

	kv.maxraftstate = maxraftstate

	kv.indexToCh = make(map[int]chan Result)
	kv.dataDict = make(map[string]string)
	kv.lastCommand = make(map[int64]int)
	// kv.lastApplied = 0

	// You may need initialization code here.
	kv.installSnapshot(persister.ReadSnapshot())
	go kv.applier()
	// go kv.snapshotter()

	return kv
}
