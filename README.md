Fixed:

- Ignore messages if term < currentTerm.
- step down / update term if term > currentTerm
- nodes don't forget who they voted for
- Candidate distinguishes between votes from same follower for the
  same Term. [and Candidates are allowed to reissue duplicate votes]

Now found:
  - Leader overwrites nextIndex and matchIndex if ClientMessages are received
    before ElectedAsLeader

  - Another, orthogonal one?: prevLogIndex has the wrong value for the second
    log entry
