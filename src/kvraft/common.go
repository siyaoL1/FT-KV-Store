package kvraft

const (
	ErrWrongLeader = "ErrWrongLeader"
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
)

type Err string

// ***************************************
// ********      RPC Structs      ********
// ***************************************

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client int64
	OpNum  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client int64
	OpNum  int
}

type GetReply struct {
	Err   Err
	Value string
}
