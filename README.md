Fixed:

- Ignore messages if term < currentTerm.
- step down / update term if term > currentTerm
- Candidate distinguishes between votes from same follower for the
  same Term.

Remaining Issue: nodes forget who they voted for
