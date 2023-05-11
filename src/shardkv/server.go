package shardkv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
	Config shardctrler.Config
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

	shardState   map[int]ShardState // stores the state of each shard
	shardsToSend []bool             // stores whether a shard needs to send to other groups
	shardsToRecv []bool             // stores whether a shard needs to receive from other groups
	config       shardctrler.Config // current config
}

type ShardState struct {
	ConfigNum   int
	DataDict    map[string]string
	LastCommand map[int64]int // stores OpNum of the last
}

// ***************************************
// ************    Config    *************
// ***************************************
// Check if the current config is the same as the previous config
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
		if currConfigNum == prevConfigNum+1 && kv.finishedMigration() {
			Debug(dKvConfig, "KV%v | Config | Previous config: %+v, Current config: %+v, prevConfigNum: %v, currConfigNum: %v\n", kv.me, String(prevConfig), String(currConfig), prevConfigNum, currConfigNum)
			// send the config to raft
			op := Op{OpType: "Config", Config: currConfig}
			kv.rf.Start(op)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Update the shards based on the new config
func (kv *ShardKV) updateShardsStatusL(prevConfig shardctrler.Config, currConfig shardctrler.Config) {
	Debug(dKvConfig, "KV%v | UpdateShardsStatus | Previous config: %+v, Current config: %+v\n", kv.me, String(prevConfig), String(currConfig))
	// Shards that are no longer in charge of
	for shard, prevGid := range prevConfig.Shards {
		currGid := currConfig.Shards[shard]
		// if the shard is in the previous config, but not in the current config, then it needs to be sent
		if prevGid == kv.gid && prevGid != currGid {
			kv.shardsToSend[shard] = true
		}
	}

	// Shards that need to be received
	for shard, currGid := range currConfig.Shards {
		prevGid := prevConfig.Shards[shard]
		// if the shard is in the current config, but not in the previous config, then it needs to be received
		if currGid == kv.gid && prevGid != currGid {
			kv.shardsToRecv[shard] = true
		}
	}
	Debug(dKvConfig, "KV%v | UpdateShardsStatus | shardsToSend: %v, shardsToRecv: %v\n", kv.me, String(kv.shardsToSend), String(kv.shardsToRecv))
}

// ***************************************
// *********** Helper Function  **********
// ***************************************
// return the string representation of the data based on the type
func String(data interface{}) string {
	switch value := data.(type) {
	case shardctrler.Config:
		config := value
		result := ""
		result += fmt.Sprintf("{ Config Num: %v ", config.Num)
		result += fmt.Sprintf(", Shards: %+v", config.Shards)
		groups, _ := json.Marshal(config.Groups)
		result += fmt.Sprintf(", Groups: %v }", string(groups))
		return result
	case []bool:
		arr := value
		strArr := make([]string, len(arr))
		for i, v := range arr {
			strArr[i] = strconv.FormatBool(v)
		}
		return fmt.Sprintf("{%v}", strings.Join(strArr, ", "))
	default:
		return ""
	}
}

func copyShardState(shardState ShardState) ShardState {
	copy := ShardState{
		ConfigNum:   shardState.ConfigNum,
		DataDict:    make(map[string]string),
		LastCommand: make(map[int64]int),
	}
	for k, v := range shardState.DataDict {
		copy.DataDict[k] = v
	}
	for k, v := range shardState.LastCommand {
		copy.LastCommand[k] = v
	}

	return copy
}

// Check if a shard is currently valid for querying
func (kv *ShardKV) isValid(key string) bool {
	shard := key2shard(key)
	// group is in charge of the shard && shard is not waiting to receive
	return kv.config.Shards[shard] == kv.gid && !kv.shardsToRecv[shard]
}

// func (kv *ShardKV) inCharge(key string) bool {
// 	shard := key2shard(key)
// 	return kv.config.Shards[shard] == kv.gid
// }

func (kv *ShardKV) finishedMigration() bool {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardsToSend[i] || kv.shardsToRecv[i] {
			return false
		}
	}

	return true
}

// ***************************************
// ********** Shards  Migration  *********
// ***************************************
func (kv *ShardKV) sendShards() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()
		if !isLeader {
			continue
		}

		// send shards to other groups
		for shard, send := range kv.shardsToSend {
			if send {
				go kv.sendShard(shard)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendShard(shard int) {
	gid := kv.config.Shards[shard]

	kv.mu.Lock()
	shardState := kv.shardState[shard]
	kv.mu.Unlock()

	// Construct args and reply
	args := &MigrationArgs{
		ShardIndex: shard,
		ShardState: copyShardState(shardState),
	}
	var reply MigrationReply

	Debug(dKvSendShard, "KV%v | SendShard | Shard: %v, to: %v\n", kv.me, shard, gid)
	for _, serverName := range kv.config.Groups[gid] {
		// send shard to one server in the group
		end := kv.make_end(serverName)
		ok := end.Call("ShardKV.ReceiveShard", args, &reply)
		// mark the shard as sent if the shard is successfully sent
		if ok && reply.Err == OK {
			kv.mu.Lock()
			kv.shardsToSend[shard] = false
			kv.mu.Unlock()
		}

	}

}

func (kv *ShardKV) ReceiveShard(args *MigrationArgs, reply *MigrationReply) {
	if kv.killed() {
		return
	}

	// check if the shard is valid
	if args.ShardState.ConfigNum != kv.config.Num || kv.config.Shards[args.ShardIndex] != kv.gid {
		*reply = MigrationReply{Err: ErrWrongGroup}
		return
	}

	// update the shard state
	kv.mu.Lock()
	kv.shardState[args.ShardIndex] = args.ShardState
	kv.shardsToRecv[args.ShardIndex] = false
	kv.mu.Unlock()

	*reply = MigrationReply{Err: OK}
}

// ***************************************
// ************  Get Handler  ************
// ***************************************

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	// config number is not updated or the shard is not valid for the group
	if args.ConfigNum != kv.config.Num || !kv.isValid(args.Key) {
		*reply = GetReply{Err: ErrWrongGroup}
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

	// config number is not updated or the shard is not valid for the group
	if args.ConfigNum != kv.config.Num || !kv.isValid(args.Key) {
		*reply = PutAppendReply{Err: ErrWrongGroup}
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
		e.Encode(kv.shardsToSend)
		e.Encode(kv.shardsToRecv)
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
	d.Decode(&kv.shardsToSend)
	d.Decode(&kv.shardsToRecv)
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
	labgob.Register(Result{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

	kv := new(ShardKV)
	kv.mu = sync.Mutex{}
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) //  order swapped due to dependency
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.dead = 0
	kv.shardCtrler = shardctrler.MakeClerk(kv.ctrlers)
	kv.indexToCh = make(map[int]chan Result)
	// init empty shard states
	kv.shardState = make(map[int]ShardState)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardState[i] = ShardState{ConfigNum: 0, DataDict: make(map[string]string), LastCommand: make(map[int64]int)}
	}
	// init empty migrating shards
	kv.shardsToSend = make([]bool, shardctrler.NShards)
	kv.shardsToRecv = make([]bool, shardctrler.NShards)
	// init empty config
	kv.config = shardctrler.Config{Num: 0, Shards: [shardctrler.NShards]int{}, Groups: map[int][]string{}}

	kv.installSnapshot(persister.ReadSnapshot())
	go kv.applier()
	go kv.checkConfig()
	go kv.sendShards()

	return kv
}
