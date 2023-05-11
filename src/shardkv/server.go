package shardkv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32               // set by Kill()
	shardCtrler *shardctrler.Clerk  // shardctrler clerk
	indexToCh   map[int]chan Result // maps op index to channel

	shardState map[int]ShardState // stores the state of each shard
	validShard map[int]bool       // stores whether a shard is valid, i.e not migrating
	config     shardctrler.Config // current config
}

type ShardState struct {
	ShardIndex  int
	DataDict    map[string]string
	LastCommand map[int64]int // stores OpNum of the last
}

// ***************************************
// ************  Get Handler  ************
// ***************************************

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
// ************ CommandValid *************
// ***************************************

func (kv *ShardKV) processCommandValid(m raft.ApplyMsg) Result {
	op := m.Command.(Op)
	var result Result

	kv.mu.Lock()
	// obtain state of the shard
	shardNum := key2shard(op.Key)
	state := kv.shardState[shardNum]
	switch op.OpType {
	case "Get":
		value, ok := state.DataDict[op.Key]
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
		Debug(dKvApGet, "KV%v | Apply Get | Applied Get: %+v, dataDict: %+v\n", kv.me, result, state.DataDict)

	case "Put", "Append":
		lastOpNum, ok := state.LastCommand[op.Client]
		// last operation doesn't exist or the current operation is newer
		if !ok || lastOpNum < op.OpNum {
			state.LastCommand[op.Client] = op.OpNum
			state.DataDict[op.Key] = op.Value
		}
		result = Result{
			Err: OK,
			Op:  op,
		}
		Debug(dKvApApd, "KV%v | Apply Put/Append | Applied result: %+v, dataDict: %+v\n", kv.me, result, state.DataDict)
	}
	kv.mu.Unlock()

	return result
}

// ***************************************
// ************    Snapshot  *************
// ***************************************
func (kv *ShardKV) checkSnapshot(index int) {
	curr_size := kv.rf.GetStateSize()
	if kv.maxraftstate != -1 && curr_size > kv.maxraftstate/2 {
		Debug(dKvSnap, "KV%v | Snapshot | Raft state size: %v, maxraftstate: %v\n", kv.me, curr_size, kv.maxraftstate)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.shardState)
		e.Encode(kv.validShard)
		data := w.Bytes()
		go kv.rf.Snapshot(index, data)
		Debug(dKvSnap, "KV%v | Snapshot | Snapshoted, current Raft state size: %v\n", kv.me, index, data, kv.rf.GetStateSize())
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	Debug(dKvSnap, "KV%v | Snapshot | Install snapshot, snapshot: %v\n", kv.me, snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.shardState)
	d.Decode(&kv.validShard)
}

// ***************************************
// ************     Apply    *************
// ***************************************

func (kv *ShardKV) apply(index int, result Result) {
	kv.mu.Lock()
	resultCh, ok := kv.indexToCh[index]
	kv.mu.Unlock()

	Debug(dKvApplier, "KV%v | resultCh | resultCh:%v, ok:%v\n", kv.me, resultCh, ok)
	if ok {
		resultCh <- result
		Debug(dKvApplier, "KV%v | applier | Sent result to resultCh: %+v\n", kv.me, resultCh)
	}
}

func (kv *ShardKV) applier() {
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

// ***************************************
// ************    Config    *************
// ***************************************
func configString(config shardctrler.Config) string {
	result := ""
	result += fmt.Sprintf("{ Config Num: %v ", config.Num)
	result += fmt.Sprintf(", Shards: %+v", config.Shards)
	groups, _ := json.Marshal(config.Groups)
	result += fmt.Sprintf(", Groups: %v }", string(groups))
	return result
}

func (kv *ShardKV) checkConfig() {
	for !kv.killed() {

		kv.mu.Lock()
		prevConfig := kv.config
		prevConfigNum := prevConfig.Num
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()
		if !isLeader {
			continue
		}
		currConfig := kv.shardCtrler.Query(-1) // query the latest config
		currConfigNum := currConfig.Num
		if currConfigNum > prevConfigNum {
			Debug(dKvConfig, "KV%v | Config | Previous config: %+v, Current config: %+v, prevConfigNum: %v, currConfigNum: %v\n", kv.me, configString(prevConfig), configString(currConfig), prevConfigNum, currConfigNum)
			kv.mu.Lock()
			kv.config = currConfig
			kv.mu.Unlock()
			// kv.updateShards(currConfig)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// func (kv *ShardKV) updateShards(currConfig shardctrler.Config) {

// }

// ***************************************
// ************     Kill    *************
// ***************************************

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	labgob.Register(Op{})
	labgob.Register(Result{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

	kv.dead = 0
	kv.indexToCh = make(map[int]chan Result)

	// Use something like this to talk to the shardctrler:
	kv.shardCtrler = shardctrler.MakeClerk(kv.ctrlers)
	kv.installSnapshot(persister.ReadSnapshot())
	go kv.applier()
	go kv.checkConfig()

	return kv
}
