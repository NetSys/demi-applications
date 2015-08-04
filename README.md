Ignore messages if term < currentTerm.

Issue: nodes forget who they voted for

TODO(cs): fix bug where nodes don't step down / update term
if term > currentTerm? Probably still triggerable without that
