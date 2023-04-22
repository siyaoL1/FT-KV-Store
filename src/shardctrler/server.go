package shardctrler

import (
	"encoding/json"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	indexToCh map[int]chan Result
	// dataDict    map[string]string
	lastCommand map[int64]int // stores OpNum of the last

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpType string
	OpNum  int
	Client int64
	Args   interface{}
}

type Result struct {
	Err Err
	Op  Op
}

// ***************************************
// ************    Join     **************
// ***************************************

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType: "Join",
		OpNum:  args.OpNum,
		Client: args.Client,
		Args:   *args,
	}

	index, _, isLeader := sc.rf.Start(op)

	// Init reply
	reply.WrongLeader = false
	reply.Err = OK

	// The Raft server is not leader
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	m, _ := json.Marshal(op.Args.(JoinArgs).Servers)
	Debug(dScJoin, "SC%v | Join | Receieved start result from raft, servers: %+v\n", sc.me, string(m))

	// Constructing result channel
	sc.mu.Lock()
	resultCh := make(chan Result)
	sc.indexToCh[index] = resultCh
	sc.mu.Unlock()

	// Waiting for result
	select {
	case result, ok := <-resultCh:
		if !ok {
			reply.Err = CHCLOSED
			return
		} else {
			reply.Err = result.Err
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = TIMEOUT
	}
	Debug(dScQuery, "SC%v | Join | Sending reply to Clerk, resultCh: %v, index: %v\n", sc.me, resultCh, index)
}

// ***************************************
// ************    Leave     **************
// ***************************************
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType: "Leave",
		OpNum:  args.OpNum,
		Client: args.Client,
		Args:   *args,
	}

	index, _, isLeader := sc.rf.Start(op)

	// Init reply
	reply.WrongLeader = false
	reply.Err = OK

	// The Raft server is not leader
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	m, _ := json.Marshal(op.Args.(LeaveArgs).GIDs)
	Debug(dScJoin, "SC%v | Leave | Receieved start result from raft, GIDs: %+v\n", sc.me, string(m))

	// Constructing result channel
	sc.mu.Lock()
	resultCh := make(chan Result)
	sc.indexToCh[index] = resultCh
	sc.mu.Unlock()

	// Waiting for result
	select {
	case result, ok := <-resultCh:
		if !ok {
			reply.Err = CHCLOSED
			return
		} else {
			reply.Err = result.Err
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = TIMEOUT
	}
	Debug(dScQuery, "SC%v | Leave | Sending reply to Clerk, resultCh: %v, index: %v\n", sc.me, resultCh, index)
}

// ***************************************
// ************    Move     **************
// ***************************************
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		OpType: "Move",
		OpNum:  args.OpNum,
		Client: args.Client,
		Args:   *args,
	}

	index, _, isLeader := sc.rf.Start(op)

	// Init reply
	reply.WrongLeader = false
	reply.Err = OK

	// The Raft server is not leader
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	m, _ := json.Marshal(op.Args.(MoveArgs).GID)
	Debug(dScJoin, "SC%v | Move | Receieved start result from raft, GID: %+v\n", sc.me, string(m))

	// Constructing result channel
	sc.mu.Lock()
	resultCh := make(chan Result)
	sc.indexToCh[index] = resultCh
	sc.mu.Unlock()

	// Waiting for result
	select {
	case result, ok := <-resultCh:
		if !ok {
			reply.Err = CHCLOSED
			return
		} else {
			reply.Err = result.Err
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = TIMEOUT
	}
	Debug(dScQuery, "SC%v | Move | Sending reply to Clerk, resultCh: %v, index: %v\n", sc.me, resultCh, index)

}

// ***************************************
// ************    Query     **************
// ***************************************
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Constructing op to send to Raft
	op := Op{
		OpType: "Query",
		OpNum:  args.Num,
	}

	index, _, isLeader := sc.rf.Start(op)

	// Init reply
	reply.WrongLeader = false
	reply.Err = OK

	// The Raft server is not leader
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	Debug(dScQuery, "SC%v | Query | Receieved start result from raft, index: %v\n", sc.me, index)

	// Constructing result channel
	sc.mu.Lock()
	resultCh := make(chan Result)
	sc.indexToCh[index] = resultCh
	sc.mu.Unlock()

	// Waiting for result
	select {
	case result, ok := <-resultCh:
		if !ok {
			reply.Err = CHCLOSED
			return
		} else {
			reply.Err = result.Err
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = TIMEOUT
	}

	// Get the corresponding config
	sc.mu.Lock()
	if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
	sc.mu.Unlock()
	Debug(dScQuery, "SC%v | Query | Sending reply to Clerk, resultCh: %v, index: %v, config: %+v\n", sc.me, resultCh, index, reply.Config)
}

// ***************************************
// ************    Helper   **************
// ***************************************

func (sc *ShardCtrler) checkDuplicateL(client int64, opNum int) bool {
	lastOpNum, ok := sc.lastCommand[client]
	return ok && lastOpNum >= opNum
}

func (sc *ShardCtrler) lastConfigL() *Config {
	return &(sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) createConfigL() {
	sc.configs = append(sc.configs, Config{})
	newConfig := &sc.configs[len(sc.configs)-1]
	oldConfig := &sc.configs[len(sc.configs)-2]
	// Init newConfig
	newConfig.Num = len(sc.configs) - 1
	newConfig.Groups = make(map[int][]string) // Need to copy map manually
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	newConfig.Shards = oldConfig.Shards
}

// ***************************************
// ************ CommandValid *************
// ***************************************
func (sc *ShardCtrler) mostAndLeastShardsL(member map[int][]int) (int, int) {
	// Get the group has the most and least number of shards
	maxGroup := -1
	minGroup := -1
	maxShards := 0
	minShards := NShards + 1
	for group, shards := range member {
		if len(shards) > maxShards {
			maxGroup = group
			maxShards = len(shards)
		}
		if len(shards) < minShards || minShards == 0 {
			minGroup = group
			minShards = len(shards)
		}
	}

	return maxGroup, minGroup
}

func (sc *ShardCtrler) balanceShardsL(config *Config) {
	// Get the assigned Shard index for each group
	member := make(map[int][]int)
	for group, _ := range config.Groups {
		member[group] = make([]int, 0)
	}
	for i, group := range config.Shards {
		member[group] = append(member[group], i)
	}
	m, _ := json.Marshal(config.Groups)
	Debug(dScApJoin, "SC%v | Apply Join | before balance config:%+v, groups:%+v\n", sc.me, config, string(m))
	maxGroup, minGroup := sc.mostAndLeastShardsL(member)
	Debug(dScApJoin, "SC%v | Apply Join | maxGroup:%v, minGroup:%v\n", sc.me, maxGroup, minGroup)

	// more than 1 group && not the same group && group has more than one difference
	for maxGroup != -1 && maxGroup != minGroup && len(member[maxGroup])-len(member[minGroup]) > 1 {
		// Move one shard over
		shard := member[maxGroup][0]
		member[maxGroup] = member[maxGroup][1:]
		member[minGroup] = append(member[minGroup], shard)
		config.Shards[shard] = minGroup

		maxGroup, minGroup = sc.mostAndLeastShardsL(member)

		m, _ := json.Marshal(config.Groups)
		Debug(dScApJoin, "SC%v | Apply Join | balancing config:%+v, groups:%+v\n", sc.me, config, string(m))
		Debug(dScApJoin, "SC%v | Apply Join | maxGroup:%v, minGroup:%v\n", sc.me, maxGroup, minGroup)
	}
}

func (sc *ShardCtrler) processCommandValid(m raft.ApplyMsg) Result {
	op := m.Command.(Op)
	var result Result

	sc.mu.Lock()
	//TODO: Implement the cases
	switch op.OpType {
	case "Join":
		if !sc.checkDuplicateL(op.Client, op.OpNum) {
			sc.lastCommand[op.Client] = op.OpNum
			sc.createConfigL()
			newConfig := sc.lastConfigL()
			// Add new group
			for gid, servers := range op.Args.(JoinArgs).Servers {
				newConfig.Groups[gid] = servers
			}
			// The first group, assign all shards to it
			if len(newConfig.Groups) == 1 {
				var gid int
				for group := range newConfig.Groups {
					gid = group
				}
				for i, _ := range newConfig.Shards {
					newConfig.Shards[i] = gid
				}
			} else {
				// Rebalance
				sc.balanceShardsL(newConfig)
			}
		}
		result = Result{Err: OK, Op: op}
		Debug(dScApJoin, "SC%v | Apply Join | result:%+v, config:%+v\n", sc.me, result, sc.lastConfigL())
	case "Leave":
		if !sc.checkDuplicateL(op.Client, op.OpNum) {
			sc.lastCommand[op.Client] = op.OpNum
			sc.createConfigL()
			newConfig := sc.lastConfigL()
			// Remove group
			for _, gid := range op.Args.(LeaveArgs).GIDs {
				delete(newConfig.Groups, gid)
			}

			// Only one group left, assign all shards to it
			if len(newConfig.Groups) == 1 {
				var gid int
				for group := range newConfig.Groups {
					gid = group
				}
				for i, _ := range newConfig.Shards {
					newConfig.Shards[i] = gid
				}
			} else {
				// Rebalance
				sc.balanceShardsL(newConfig)
			}
		}
		result = Result{Err: OK, Op: op}
		Debug(dScApLeave, "SC%v | Apply Leave | : %+v\n", sc.me, result)
	case "Move":
		if !sc.checkDuplicateL(op.Client, op.OpNum) {
			sc.lastCommand[op.Client] = op.OpNum
			sc.createConfigL()
			newConfig := sc.lastConfigL()
			// Move shard
			shard := op.Args.(MoveArgs).Shard
			gid := op.Args.(MoveArgs).GID
			newConfig.Shards[shard] = gid
		}
		result = Result{Err: OK, Op: op}
		Debug(dScApMove, "SC%v | Apply Move | : %+v\n", sc.me, result)
	case "Query":
		// NOTE: Query is a read-only operation so don't need to keep track of lastCommand
		result = Result{Err: OK, Op: op}
		Debug(dScApQuery, "SC%v | Apply Query | : %+v\n", sc.me, result)
	}
	bla, _ := json.Marshal(sc.lastConfigL().Groups)
	Debug(dScApplier, "SC%v | Applier | After Apply, config:%+v, groups:%+v\n", sc.me, sc.lastConfigL(), string(bla))

	sc.mu.Unlock()

	return result
}

// ***************************************
// ************     Apply    *************
// ***************************************
func (sc *ShardCtrler) apply(index int, result Result) {
	sc.mu.Lock()
	resultCh, ok := sc.indexToCh[index]
	sc.mu.Unlock()

	Debug(dScApplier, "SC%v | resultCh | resultCh:%v, ok:%v\n", sc.me, resultCh, ok)
	if ok {
		resultCh <- result
		Debug(dScApplier, "SC%v | applier | Sent result to resultCh: %+v\n", sc.me, resultCh)
	}
}

func (sc *ShardCtrler) applier() {
	for {
		Debug(dScApplier, "SC%v | applier | Waiting for message on applych: %v\n", sc.me, sc.applyCh)
		m := <-sc.applyCh
		Debug(dScApplier, "SC%v | applier | Received message from applyCh: %+v\n", sc.me, m)

		if m.CommandValid {
			result := sc.processCommandValid(m)
			sc.apply(m.CommandIndex, result)
		}
		// no need to snapshot
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.indexToCh = make(map[int]chan Result)
	sc.lastCommand = make(map[int64]int)
	// init first config
	sc.balanceShardsL(&sc.configs[0])

	go sc.applier()

	return sc
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
