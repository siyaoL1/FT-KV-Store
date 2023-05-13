## Reconfiguration Process

- The go routine detects that there's a reconfiguration happened.
  1. It stops responding to any of the query until reconfiguration is finished.

* Send raft a reconfig operation to make sure all the raft servers are at the same stage.
* After raft responded OK
  1. Store the shards that need to send to the others.
  2. Check whether all shards has been sent to the new servers

     1. Send out RPCs to send the shards
  3. Send out RPCs to send the shards
  4. Send out RPCs to send the shards
  5. In the meanwhile

     1. Reject all the queries when not all shards are sent and received.


- Send raft the move shard op
- Once received reply from raft saying the command is replicated,
  - update shard data
  - and also unset shardToRecv flag for the shard


## Question

- If one server gets all the shards that they needs for the new config, cna it continue to execute?
- If
