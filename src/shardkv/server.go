package shardkv

import (
	"bytes"
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
	OpType     string
	Key        string
	Value      string
	OpNum      int
	Client     int64
	Config     shardctrler.Config
	ShardNum   int
	ShardState ShardState
	ConfigNum  int
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
	// stuff to snapshot
	shardState   map[int]ShardState // stores the state of each shard
	shardsToRecv []bool             // stores whether a shard needs to receive from other groups
	config       shardctrler.Config // current config
	prevConfig   shardctrler.Config // previous config
}

type ShardState struct {
	DataDict    map[string]string
	LastCommand map[int64]int // stores OpNum of the last
}

// ***************************************
// *********** Helper Function  **********
// ***************************************

func equal(opOne Op, opTwo Op) bool {
	return opOne.OpNum == opTwo.OpNum && opOne.Client == opTwo.Client &&
		opOne.OpType == opTwo.OpType && opOne.Key == opTwo.Key && opOne.Value == opTwo.Value
}

func copyShardState(shardState ShardState) ShardState {
	copy := ShardState{
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

// Check if a key is currently valid for querying
func (kv *ShardKV) keyIsValid(key string) bool {
	shard := key2shard(key)
	// group is in charge of the shard && shard is not waiting to receive
	return kv.config.Shards[shard] == kv.gid && !kv.shardsToRecv[shard]
}

// Check if a shard is currently valid for querying
func (kv *ShardKV) shardIsValid(shard int) bool {
	// group is in charge of the shard && shard is not waiting to receive
	return kv.config.Shards[shard] == kv.gid && !kv.shardsToRecv[shard]
}

// func (kv *ShardKV) inCharge(key string) bool {
// 	shard := key2shard(key)
// 	return kv.config.Shards[shard] == kv.gid
// }

func (kv *ShardKV) finishedMigration() bool {
	result := false
	for i := 0; i < shardctrler.NShards; i++ {
		result = result || kv.shardsToRecv[i]
	}

	return !result
}

// ***************************************
// ********** Shards  Migration  *********
// ***************************************

// Update the shards based on the new config
func (kv *ShardKV) updateShardsStatusL() {
	prevConfig := kv.prevConfig
	currConfig := kv.config
	if prevConfig.Num == 0 {
		return
	}
	// Debug(dKvConfig, "KV%v-%v | UpdateShardsStatus | Previous config: %+v, Current config: %+v\n", kv.gid, kv.me, String(prevConfig), String(currConfig))
	// Shards that need to be received
	for shard, currGid := range currConfig.Shards {
		prevGid := prevConfig.Shards[shard]
		// if the shard is in the current config, but not in the previous config, then it needs to be received
		if currGid == kv.gid && prevGid != currGid {
			kv.shardsToRecv[shard] = true
		}
	}
	Debug(dKvConfig, "KV%v-%v | UpdateShardsStatus | shardsToRecv: %v\n", kv.gid, kv.me, String(kv.shardsToRecv))
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
		kv.mu.Unlock()

		currConfig := kv.shardCtrler.Query(prevConfigNum + 1) // query the latest config
		currConfigNum := currConfig.Num
		op := Op{
			OpType: "Config",
			Config: currConfig,
		}

		if currConfigNum != prevConfigNum+1 || !kv.finishedMigration() {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// send the config to raft
		index, _, isLeader := kv.rf.Start(op)

		// The Raft server is not leader
		if !isLeader {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		Debug(dKvConfig, "KV%v-%v | Config | Config Outdated. Previous config: %+v, Current config: %+v, prevConfigNum: %v, currConfigNum: %v\n", kv.gid, kv.me, String(prevConfig), String(currConfig), prevConfigNum, currConfigNum)
		Debug(dKvConfig, "KV%v-%v | Config | Receieved raft start() result, index: %v\n", kv.gid, kv.me, index)

		// Create channel and wait for response from channel
		kv.mu.Lock()
		resultCh := make(chan Result)
		kv.indexToCh[index] = resultCh
		kv.mu.Unlock()

		Debug(dKvConfig, "KV%v-%v | Config | Waiting for raft apply result from resultCh: %+v\n", kv.gid, kv.me, resultCh)
		var result Result
		var ok bool
		select {
		case result, ok = <-resultCh:
			if ok && result.Err == OK && equal(result.Op, op) {
				Debug(dKvConfig, "KV%v-%v | Config | Updated config from %+v to %+v, shardsToRecv: %+v\n", kv.gid, kv.me, String(prevConfig), String(currConfig), String(kv.shardsToRecv))
			} else {
				Debug(dKvConfig, "KV%v-%v | Config | Received wrong result from resultCh: %+v\n", kv.gid, kv.me, String(result))
			}
		case <-time.After(1000 * time.Millisecond):
			Debug(dKvConfig, "KV%v-%v | Config | Timeout waiting for result from resultCh\n", kv.gid, kv.me)
		}
		// Delete the used index
		kv.mu.Lock()
		delete(kv.indexToCh, index)
		kv.mu.Unlock()
	}
}

func copyConfig(config shardctrler.Config) shardctrler.Config {
	newConfig := shardctrler.Config{
		Num:    config.Num,
		Shards: [shardctrler.NShards]int{},
		Groups: map[int][]string{},
	}
	for i := 0; i < shardctrler.NShards; i++ {
		newConfig.Shards[i] = config.Shards[i]
	}
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
	}
	return newConfig
}

// ***************************************
// ***********  RequestShard  ************
// ***************************************
// Background routine that checks if there are shards to be received
func (kv *ShardKV) requestShards() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// pull shards from other groups
		for shard, recv := range kv.shardsToRecv {
			if recv {
				go kv.requestShard(shard)
			}
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// send out a request to other groups to get the shard
func (kv *ShardKV) requestShard(shard int) {
	gid := kv.prevConfig.Shards[shard]
	Debug(dKvRequestShard, "KV%v-%v | RequestShard | Requesting Shard: %v, from: %v\n", kv.gid, kv.me, shard, gid)

	kv.mu.Lock()
	config := kv.config
	prevConfig := kv.prevConfig
	kv.mu.Unlock()

	// Construct args and reply
	args := &RequestShardArgs{
		ConfigNum:  config.Num,
		ShardIndex: shard,
	}

	var reply RequestShardReply
	// print config.Groups[gid]
	// Debug(dKvRequestShard, "KV%v-%v | RequestShard | config.Groups[gid]: %+v\n", kv.gid, kv.me, config.Groups[gid])
	for _, serverName := range prevConfig.Groups[gid] {
		// send shard to one server in the group
		end := kv.make_end(serverName)
		Debug(dKvRequestShard, "KV%v-%v | RequestShard | Sending request for shard: %v to %v-%v\n", kv.gid, kv.me, shard, gid, serverName)
		ok := end.Call("ShardKV.RequestShard", args, &reply)
		if ok && reply.Err == OK {
			Debug(dKvRequestShard, "KV%v-%v | RequestShard | Received Shard: %v, from: %v\n", kv.gid, kv.me, shard, gid)
			kv.updateShard(shard, reply)
		}
	}
}

// RPC handler for requesting shard from other groups
func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	if kv.killed() {
		return
	}
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
	if !isLeader {
		// Debug(dKvRequestShard, "KV%v-%v | RequestShard | Not leader\n", kv.gid, kv.me)
		*reply = RequestShardReply{Err: ErrWrongLeader}
		return
	}
	// Debug(dKvRequestShard, "KV%v-%v | RequestShard | Received request of Shard: %v, with ConfigNum: %v\n", kv.gid, kv.me, args.ShardIndex, args.ConfigNum)

	// check if the config is up to date
	kv.mu.Lock()
	if args.ConfigNum > kv.config.Num {
		Debug(dKvRequestShard, "KV%v-%v | RequestShard | ConfigNum: %v is not valid, current ConfigNum: %v\n", kv.gid, kv.me, args.ConfigNum, kv.config.Num)
		*reply = RequestShardReply{Err: ErrWrongGroup}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// // check if the shard is up to date
	// if kv.shardsToRecv[args.ShardIndex] {
	// 	Debug(dKvRequestShard, "KV%v-%v | RequestShard | Shard: %v is not valid\n", kv.gid, kv.me, args.ShardIndex)
	// 	*reply = RequestShardReply{Err: ErrWrongGroup}
	// 	return
	// }

	kv.mu.Lock()
	// store the shard in the reply
	*reply = RequestShardReply{
		Err:        OK,
		ShardState: kv.shardState[args.ShardIndex],
	}
	kv.mu.Unlock()
	Debug(dKvRequestShard, "KV%v-%v | RequestShard | Sending Shard: %v, request's ConfigNum: %v\n", kv.gid, kv.me, args.ShardIndex, args.ConfigNum)
}

// ***************************************
// ***********  UpdateShard  *************
// ***************************************

// update ShardState based on the reply from the requestShard RPC
func (kv *ShardKV) updateShard(shard int, reply RequestShardReply) {
	Debug(dKvUpdateShard, "KV%v-%v | UpdateShard | Shard: %v, from: %v\n", kv.gid, kv.me, shard, shard)
	// send raft command to update the shard
	op := Op{
		OpType:     "UpdateShard",
		ShardNum:   shard,
		ShardState: reply.ShardState,
		ConfigNum:  kv.config.Num,
	}

	index, _, isLeader := kv.rf.Start(op)
	// this is not expected to happen since we already checked leadership before calling this function
	if !isLeader {
		return
	}
	Debug(dKvUpdateShard, "KV%v-%v | UpdateShard | Receieved start result from raft, index: %v\n", kv.gid, kv.me, index)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	kv.mu.Unlock()

	var result Result
	var ok bool
	select {
	case result, ok = <-resultCh:
	case <-time.After(1000 * time.Millisecond):
	}

	// Delete the used index
	kv.mu.Lock()
	delete(kv.indexToCh, index)
	kv.mu.Unlock()
	Debug(dKvGet, "KV%v-%v | UpdateShard | UpdateShard result, ok: %v, Err: %v, result: %+v\n", kv.gid, kv.me, ok, result.Err, result)

}

// ***************************************
// ************  Get Handler  ************
// ***************************************

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	// The shard is not valid for the group
	if !kv.keyIsValid(args.Key) {
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
	Debug(dKvGet, "KV%v-%v | Get | Receieved start result from raft, index: %v\n", kv.gid, kv.me, index)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	kv.mu.Unlock()

	var result Result
	var ok bool
	select {
	case result, ok = <-resultCh:
		if !ok || result.Err == ErrWrongLeader || !equal(result.Op, op) {
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
	Debug(dKvGet, "KV%v-%v | Get | Sending result to Clerk, key: %v, shard data: %+v, err: %+v, index: %v\n", kv.gid, kv.me, args.Key, kv.shardState[key2shard(args.Key)].DataDict, reply.Err, index)
}

// ***************************************
// ********  Put/Append Handler  *********
// ***************************************

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Debug(,"KV%v-%v | PutAppend | Received RPC with args: %+v\n",kv.gid, kv.me, args)
	// Your code here.
	if kv.killed() {
		return
	}

	// The shard is not valid for the group
	if !kv.keyIsValid(args.Key) {
		Debug(dKvPutApp, "KV%v-%v | PutAppend | Replying ErrWrongGroup, args: %+v, shard: %+v\n", kv.gid, kv.me, *args, key2shard(args.Key))
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

	Debug(dKvPutApp, "KV%v-%v | PutAppend | Receieved start result from raft leader, index: %v\n", kv.gid, kv.me, index)
	// Debug(dKvPutApp, "KV%v-%v | PutAppend | Raft: %+v,\n",kv.gid, kv.me, kv.rf)

	// Create channel and wait for response from channel
	kv.mu.Lock()
	resultCh := make(chan Result)
	kv.indexToCh[index] = resultCh
	// Debug(dKvPutApp, "KV%v-%v | resultCh | indexToCh:%+v, index:%v\n", kv.gid, kv.me, kv.indexToCh, index)
	kv.mu.Unlock()

	// Debug(dKvPutApp, "KV%v-%v | PutAppend | Waiting for raft apply result from resultCh\n", kv.gid, kv.me)
	var result Result
	var ok bool
	select {
	case result, ok = <-resultCh:
		// if result.
		if !ok || result.Err == ErrWrongLeader || !equal(result.Op, op) {
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
	Debug(dKvPutApp, "KV%v-%v | PutAppend | Sending result to Clerk, key: %v, shard data: %+v, Err: %+v, index: %v\n", kv.gid, kv.me, args.Key, kv.shardState[key2shard(args.Key)].DataDict, reply.Err, index)
	kv.mu.Lock()
	delete(kv.indexToCh, index)
	kv.mu.Unlock()
}

// ***************************************
// ************ CommandValid *************
// ***************************************

func (kv *ShardKV) processCommandValid(m raft.ApplyMsg) Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := m.Command.(Op)
	var result Result

	// obtain state of the shard
	shardNum := key2shard(op.Key)
	state := kv.shardState[shardNum]

	switch op.OpType {
	case "Get":
		value, ok := state.DataDict[op.Key]

		if !kv.shardIsValid(shardNum) {
			result = Result{
				Err:   ErrWrongGroup,
				Value: "",
				Op:    op,
			}
		} else {
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
			Debug(dKvApGet, "KV%v-%v | Apply Get | key: %+v, value: %+v dataDict: %+v\n", kv.gid, kv.me, op.Key, op.Value, state.DataDict)
		}
	case "Put", "Append":
		lastOpNum, ok := state.LastCommand[op.Client]
		if !kv.shardIsValid(shardNum) {
			result = Result{
				Err:   ErrWrongGroup,
				Value: "",
				Op:    op,
			}
		} else {
			// last operation doesn't exist or the current operation is newer
			if !ok || lastOpNum < op.OpNum {
				state.LastCommand[op.Client] = op.OpNum
				if op.OpType == "Put" {
					state.DataDict[op.Key] = op.Value
				} else {
					state.DataDict[op.Key] += op.Value
				}
				Debug(dKvApApd, "KV%v-%v | Apply Put/Append | key: %+v, value: %+v dataDict: %+v\n", kv.gid, kv.me, op.Key, op.Value, state.DataDict)
			}
			result = Result{
				Err: OK,
				Op:  op,
			}
		}
	case "Config":
		// update the config
		if kv.config.Num < op.Config.Num {
			Debug(dKvApConfig, "KV%v-%v | Apply Config | config, old: %+v, new: %+v\n", kv.gid, kv.me, String(kv.config), String(op.Config))
			kv.prevConfig = copyConfig(kv.config)
			kv.config = copyConfig(op.Config)
			kv.updateShardsStatusL()
			result = Result{
				Err: OK,
				Op:  op,
			}
		} else {
			result = Result{
				Err: ErrWrongGroup,
				Op:  op,
			}
		}
	case "UpdateShard":
		// update the shard
		// if config number matches and the shard is expected to be received
		if kv.config.Num == op.ConfigNum && kv.shardsToRecv[op.ShardNum] {
			Debug(dKvApUpdateShard, "KV%v-%v | Apply UpdateShard | ShardNum: %v, ShardState: %+v\n", kv.gid, kv.me, op.ShardNum, op.ShardState)
			kv.shardState[op.ShardNum] = op.ShardState
			kv.shardsToRecv[op.ShardNum] = false
			result = Result{
				Err: OK,
				Op:  op,
			}
		} else {
			result = Result{
				Err: ErrWrongGroup,
				Op:  op,
			}
		}
	}

	return result
}

// ***************************************
// ************    Snapshot  *************
// ***************************************
func (kv *ShardKV) checkSnapshot(index int) {
	curr_size := kv.rf.GetStateSize()
	if kv.maxraftstate != -1 && curr_size > kv.maxraftstate/2 {
		Debug(dKvSnap, "KV%v-%v | Snapshot | Raft state size: %v, maxraftstate: %v\n", kv.gid, kv.me, curr_size, kv.maxraftstate)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.shardState)
		e.Encode(kv.shardsToRecv)
		e.Encode(kv.config)
		e.Encode(kv.prevConfig)
		data := w.Bytes()
		go kv.rf.Snapshot(index, data)
		Debug(dKvSnap, "KV%v-%v | Snapshot | Snapshoted, current Raft state size: %v\n", kv.gid, kv.me, index, data, kv.rf.GetStateSize())
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	Debug(dKvSnap, "KV%v-%v | Snapshot | Install snapshot, snapshot: %v\n", kv.gid, kv.me, snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.shardState)
	d.Decode(&kv.shardsToRecv)
	d.Decode(&kv.config)
	d.Decode(&kv.prevConfig)
}

// ***************************************
// ************     Apply    *************
// ***************************************

func (kv *ShardKV) apply(index int, result Result) {
	kv.mu.Lock()
	resultCh, ok := kv.indexToCh[index]
	kv.mu.Unlock()

	// Debug(dKvApplier, "KV%v-%v | resultCh | resultCh:%v, ok:%v\n", kv.gid, kv.me, resultCh, ok)
	if ok {
		resultCh <- result
		// Debug(dKvApplier, "KV%v-%v | applier | Sent result to resultCh: %+v\n", kv.gid, kv.me, resultCh)
	}
}

func (kv *ShardKV) applier() {
	for {
		// Debug(dKvApplier, "KV%v-%v | applier | Waiting for message on applych: %v\n", kv.gid, kv.me, kv.applyCh)
		m := <-kv.applyCh
		Debug(dKvApplier, "KV%v-%v | applier | Received message from applyCh: %+v\n", kv.gid, kv.me, String(m))

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
		kv.shardState[i] = ShardState{DataDict: make(map[string]string), LastCommand: make(map[int64]int)}
	}
	// init empty migrating shards
	kv.shardsToRecv = make([]bool, shardctrler.NShards)
	// init empty config
	kv.config = shardctrler.Config{Num: 0, Shards: [shardctrler.NShards]int{}, Groups: map[int][]string{}}
	kv.prevConfig = shardctrler.Config{Num: -1, Shards: [shardctrler.NShards]int{}, Groups: map[int][]string{}}

	kv.installSnapshot(persister.ReadSnapshot())
	go kv.applier()
	go kv.checkConfig()
	go kv.requestShards()

	return kv
}
