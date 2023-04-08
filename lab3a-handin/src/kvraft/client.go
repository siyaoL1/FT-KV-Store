package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

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
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me     int64
	mu     sync.Mutex
	leader int
	opNum  int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
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
//
// you can send an RPC with code like this:
//
//	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("C%v | Get | key:%v\n", ck.me%100, key)
	// Construct Args for Get RPC
	ck.mu.Lock()
	ck.opNum += 1
	args := GetArgs{
		Key:    key,
		Client: ck.me,
		OpNum:  ck.opNum,
	}
	reply := GetReply{}
	ck.mu.Unlock()

	server := ck.leader
	for { // keeps trying forever if error occured
		ok := ck.sendGet(server, &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			DPrintf("C%v | Get | Received OK reply: %+v\n", ck.me%100, reply)
			ck.leader = server
			return reply.Value
		}
		server = (server + 1) % len(ck.servers)
	}
}

// Send a AppendEntries RPC to a kvserver
func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)

	if !ok {
		Debug(dClerk, "C%v | Get RPC failed | Err:%v\n", ck.me, reply.Err)
	}
	return ok
}

// ***************************************
// ********    Put/Append RPC    *********
// ***************************************
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("C%v | PutAppend | key:%v, value: |%v|\n", ck.me%100, key, value)
	// Construct Args for PutAppend RPC
	ck.mu.Lock()
	ck.opNum += 1
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Client: ck.me,
		OpNum:  ck.opNum,
	}
	reply := PutAppendReply{}
	ck.mu.Unlock()

	for { // keeps trying forever if error occured
		ok := ck.sendPutAppend(ck.leader, &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			DPrintf("C%v | PutAppend | Received OK reply from Raft: %+v\n", ck.me%100, reply)
			return
		}
		// DPrintf("C%v | PutAppend | S%v is not leader\n", ck.me%100, server)
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

// Send a PutAppend RPC to a kvserver
func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)

	if !ok {
		DPrintf("C%v | PutAppend RPC failed | Err:%v\n", ck.me%100, reply.Err)
	}
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
