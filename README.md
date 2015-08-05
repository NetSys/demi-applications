Fixed:

- Ignore messages if term < currentTerm.
- step down / update term if term > currentTerm
- nodes don't forget who they voted for
- Candidate distinguishes between votes from same follower for the
  same Term.

Now try to find bugs in log management.
