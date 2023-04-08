package kvraft

import (
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
}

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
	DPrintf("KV%v | Get | Receieved start result from raft, index: %v\n", kv.me, index)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	kv.mu.Unlock()

	select {
	case result, ok := <-resultCh:
		DPrintf("KV%v | PutAppend | Received message from resultCh: %+v\n", kv.me, result)
		if !ok || result.Err == ErrWrongLeader {
			*reply = GetReply{
				Err: ErrWrongLeader,
			}
		} else {
			*reply = GetReply(result)
		}
	case <-time.After(1000 * time.Millisecond):
		*reply = GetReply{Err: ErrWrongLeader}
	}
}

// PutAppend RPC handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// DPrintf("KV%v | PutAppend | Received RPC with args: %+v\n", kv.me, args)
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

	DPrintf("KV%v | PutAppend | Receieved start result from raft, index: %v\n", kv.me, index)
	DPrintf("KV%v | PutAppend | Raft: %+v,\n", kv.me, kv.rf)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	kv.mu.Unlock()
	DPrintf("KV%v | resultCh | indexToCh:%+v, index:%v\n", kv.me, kv.indexToCh, index)

	DPrintf("KV%v | PutAppend | Start waiting for resultCh message\n", kv.me)
	select {
	case result, ok := <-resultCh:
		DPrintf("KV%v | PutAppend | Received message from resultCh: %+v\n", kv.me, result)
		// if result.
		if !ok || result.Err == ErrWrongLeader {
			*reply = PutAppendReply{
				Err: ErrWrongLeader,
			}
		} else {
			*reply = PutAppendReply{
				Err: result.Err,
			}
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	// DPrintf("KV%v | PutAppend | Replying to Clerk: %+v\n", kv.me, reply)
}

func (kv *KVServer) checkDuplicateL(client int64, opNum int) bool {
	lastOpNum, ok := kv.lastCommand[client]
	return ok && lastOpNum >= opNum
}

func (kv *KVServer) applier() {
	for {
		DPrintf("KV%v | applier | Waiting for message on applych: %v\n", kv.me, kv.applyCh)
		m := <-kv.applyCh
		DPrintf("KV%v | applier | Received message from applyCh: %+v\n", kv.me, m)

		if m.SnapshotValid {
			continue
		} else if m.CommandValid {
			op := m.Command.(Op)
			var result Result
			DPrintf("KV%v | applier | 1\n", kv.me)

			kv.mu.Lock()
			DPrintf("KV%v | applier | 2\n", kv.me)
			switch op.OpType {
			case "Get":
				value, ok := kv.dataDict[op.Key]
				if ok { // key exist
					result = Result{
						Err:   OK,
						Value: value,
					}
				} else { // key doesn't exist
					result = Result{
						Err:   ErrNoKey,
						Value: "",
					}
				}
			case "Put":
				DPrintf("KV%v | applier | 3\n", kv.me)
				if !kv.checkDuplicateL(op.Client, op.OpNum) {
					kv.lastCommand[op.Client] = op.OpNum
					kv.dataDict[op.Key] = op.Value
				}
				DPrintf("KV%v | applier | 4\n", kv.me)
				result = Result{
					Err: OK,
				}
			case "Append":
				if !kv.checkDuplicateL(op.Client, op.OpNum) {
					kv.lastCommand[op.Client] = op.OpNum
					kv.dataDict[op.Key] += op.Value
				}
				result = Result{
					Err: OK,
				}
			}
			kv.mu.Unlock()

			DPrintf("KV%v | applier | 5\n", kv.me)
			resultCh, ok := kv.indexToCh[m.CommandIndex]

			DPrintf("KV%v | resultCh | resultCh:%v\n", kv.me, resultCh)
			if ok {
				resultCh <- result
			}
			DPrintf("KV%v | applier | Sent result from applych: %+v\n", kv.me, kv.applyCh)

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

	// You may need initialization code here.
	go kv.applier()

	return kv
}
