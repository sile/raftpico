raftpico
========

Minimal, but practical Raft framewark ...

Feature
-------

- Dynamic membership
- Snapshot
- Pipelining
- Various statistics to inspect server's internal state
- Can be achieved by the user side
  - Effects
  - ~ConditionalLocalQuery (delay response until some conditions are met)~
- Gurantee that local queries after a command to the same server are equal to or newer index than the command's index

TODO
---

- Effect example
- File storage
- Remove unnecessary JSON serialization / deserialization when sending / receiving messages
