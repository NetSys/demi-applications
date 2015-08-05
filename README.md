Fixed:

- Ignore messages if term < currentTerm.
- step down / update term if term > currentTerm
- nodes don't forget who they voted for

[Also: allow peers to resend votes for the same candidate, i.e. deal with
drops]

But one remaining issue:

- Candidate does not distinguish between votes from same follower for the
  same Term. If the peer resends a Vote, it can incorrectly cause a Candidate
  to become elected.
