Fixed:

- Ignore messages if term < currentTerm.
- step down / update term if term > currentTerm

Remaining Issue: nodes forget who they voted for
