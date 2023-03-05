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
  - TF suggest record the last time received a RequestVote RPC or AppendEntries RPC(heartbeat)


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

This time we need to create go routine by ourselves to send out message concurrently.

Make:
  - In make, we first initiate a goroutine running tick() function 
tick:
  - Tick() is a background program that checks if a certain action is needed for a server
  1. When it is a follower it checks whether the current server need to start a election. 
    - If so create threads that starts goroutines to call sendRequestVote() to all servers
  2. When it is a candidate, it checks whether within the election timeout, it receives enough votes 
    - If so it becomes the leader 
    - If not then it starts the election again in a new term
  3. When it is a leader, it sends out heartbeats to all servers periodically.
     - We achieve periodically by creating go routines to send out heartbeat to all server and then time.sleep for 100 milliseconds.
sendRequestVote:
  - sendRequestVote() first use rf.peers[server].Call() to send RequestVote to other server and wait for a response.
  - Then it process the result, by first locks then add votes to rf if needed.

  


Cases
- When all servers wants to be become leader at the same time, no one becomes leader.
  - One server will timeout first then send RPC request to other candidates
    - Follow the rule:
      - If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower 
    - The other servers reset to follower and then grant vote.





# 2B
- After a leader is elected, it starts to receive command to append by Start()
  - Append the log to the log[]
  - Start sending the appendentries based on the nextIndex[]
    - Within a timeout, send the log based on the nextIndex
      - If get a success then update the matchIndex[]
      - If get a failure
      - If timeout, resend the same log

- AppendEntries RPC contains
  - commited index
    - so that servers can know when it is safe to apply the log.

Log entry contains:
- command
- term nunber
- index position

Question:
- When does the client comes to play, where we wants to execute the command, if there's duplicated command do we need to deal with that?
- When and how to apply?
- When starting out, there's no log in any of the followers, how are we managing to check the previous log index
  - assume that there's a 0 term and 0 index for each of the server
- Can you go over a bit about the general strategy to handle the case when the appendentries return failure due to inconsistency?
  - The optimization is not required for this lab.
- Optimization: The rule of reply for the rejection, why do we need the term and also the index 
- Later on we are having logs being compacted into snapshots right? I'm a bit worried about the indexing after removing some of the 


- Is sending one entry good enough?
- Understand the first test
- 
- how to apply? Just send the command through channel?



Initialization Procedure: (after election)
- Set nextIndex[] to be leader's last log index + 1
- Set matchIndex[] to be 0s

Broadcast Procedure:
- if leader's lastLogIndex >= nextIndex for a follower:
  - send AppendEntry to the server
- else
  - send heartbeat to the server
- Then wait for the response
  - if success:
    - update the nextIndex and matchIndex
    - if matchIndex of the follower > commitIndex:
      - if majority of the followers have >= matchIndex:
        - Update the commit index
  - if failure:
    - if reply term is greater than args term (meaning there's log inconsistency)
      - update the nextIndex -= 1
    - if reply term is smaller than args term (term of the leader is outdated)
      - update the term to be reply's term and convert to follower.

Broadcast Handler:
- If args.term < currentTerm or args.PrevLogIndex with args.PrevLogTerm doesn't exist:
  - return failure
- If it is a old index:
  - delete the existing entry in the index and the one after it.
- Append the log
- Update the lastLogIndex and lastLogTerm
- If leaderCommit > commitIndex:
  - set commitIndex = min(leaderCommit, lastLogIndex)

Note that the leader can only commit the logs made in its own term.