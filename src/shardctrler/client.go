package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

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
	// Your data here.
	me     int64
	mu     sync.Mutex
	leader int
	opNum  int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.me = nrand()
	ck.mu = sync.Mutex{}
	ck.leader = 0
	ck.opNum = 0
	return ck
}

// ***************************************
// **********    Query RPC    ************
// ***************************************

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	ck.opNum += 1
	args := &QueryArgs{
		Num:    num,
		Client: ck.me,
		OpNum:  ck.opNum,
	}
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ***************************************
// ************    Join RPC    ***********
// ***************************************

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	ck.opNum += 1
	args := &JoinArgs{
		Servers: servers,
		Client:  ck.me,
		OpNum:   ck.opNum,
	}
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ***************************************
// ***********    Leave RPC    ***********
// ***************************************

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ***************************************
// ***********    Move RPC    ************
// ***************************************

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
