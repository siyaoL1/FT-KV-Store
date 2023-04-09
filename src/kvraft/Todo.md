Client.go
- Add RPC-sending code to the Clerk Put/Append/Get methods in client.go

Server.go
- Implement PutAppend() and Get() RPC handlers in server.go
  - Handlers should enter an Op in the Raft log using Start()
    - kvservers will need to wait for Raft to complete agreement by reading applyCh
  - Need to fill in the Op struct definition in server.go so that it describes a Put/Append/Get operation
  - Execute Op commands as Raft commits them, i.e. as they are sent on the applyCh
  - Then reply to the RPC from client.