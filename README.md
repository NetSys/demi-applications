Ignore messages if term < currentTerm.

Also fix bug: don't forget who we voted for.

But: still keep the bug where nodes don't step down / update term
if term > currentTerm
