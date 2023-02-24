- How does Raft servers talk to each other?
  - See the example request vote functino, which utilizes the labrpc (a channel based rpc implementation)
- How to utilize labgod package to show warning.



- What's the difference between the RPC machanism we used in the lab1 vs lab2
  - channel based vs net/rpc package

- How does Raft object process RPC messages?
  - Does it process it concurrently?

- Why do we need a background goroutine checking whether a new election should be started

- How to reset time?
  - My idea is to simply store a status of "receivedHeartbeat int" then when ever a heartbeat is received, after timeout we don't start election, otherwise start election


Election procedure:
1. Servers start up with no leader
2. Servers waits for a random amount of time and timeout to send RequestVote RPCs to all other servers
3. One gets a majority votes from poll, then becomes leader, otherwise all waits for a random amount of time again until start election again

Leader:
1. Periodically send out heartbeats (AppendEntries RPC) to the followers to let them know that leader is alive.

Follower:
1. Wait for a random amount of time to start new election.
2. If during wait received heartbeat, reset waiting.

- heartbeat rate: 10/s
    - Test cases have server numbers of 3, 5, 7, hence my choice is to 
      - send a heartbeat to all servers and then wait for 1 sec.
- timeout:
  - 500 ~ 800 miliseconds