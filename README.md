Ignore messages if term < currentTerm.

But: still keep the bug where nodes don't step down / update term
if term > currentTerm
