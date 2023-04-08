Client.go
- Add RPC-sending code to the Clerk Put/Append/Get methods in client.go

Server.go
- Implement PutAppend() and Get() RPC handlers in server.go
  - Handlers should enter an Op in the Raft log using Start()
  - Need tofill in the Op struct definition in server.go