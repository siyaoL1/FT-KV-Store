package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// ***************************************
// ********         Clerk 		  ********
// ***************************************

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me     int64
	mu     sync.Mutex
	leader int
	opNum  int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = nrand()
	ck.mu = sync.Mutex{}
	ck.leader = 0
	ck.opNum = 0
	return ck
}

// ***************************************
// ************    Get RPC    ************
// ***************************************

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	// Construct Args for Get RPC
	ck.mu.Lock()
	ck.opNum += 1
	args := GetArgs{
		Key:    key,
		Client: ck.me,
		OpNum:  ck.opNum,
	}
	ck.mu.Unlock()

	Debug(dClerk, "C%v | Get | Request key:%v, opNum: %v, shard:%v\n", ck.me%100, key, args.OpNum, key2shard(key))

	for {
		// get the shard number for the data key
		shard := key2shard(key)
		// get the group number for the shard
		gid := ck.config.Shards[shard]
		// get the servers for the group
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					Debug(dClerk, "C%v | Get | Received reply from %+v-%v: key:%v, value: %+v, opNum: %v, shard: %v\n", ck.me%100, gid, si, args.Key, reply.Value, args.OpNum, key2shard(key))
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.opNum += 1
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Client: ck.me,
		OpNum:  ck.opNum,
	}
	ck.mu.Unlock()

	Debug(dClerk, "C%v | PutAppend | Request key:%v, value:%v, op:%v, opNum: %v, shard:%v\n", ck.me%100, key, value, op, args.OpNum, key2shard(key))

	for {
		// get the shard number for the data key
		shard := key2shard(key)
		// get the group number for the shard
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					Debug(dClerk, "C%v | PutAppend | Received reply from %+v-%v: op: %v, key:%+v, value:%+v, opNum: %v, shard: %v\n", ck.me%100, gid, si, args.Op, args.Key, args.Value, args.OpNum, key2shard(key))
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
