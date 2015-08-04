Fix the following bug: step down / update term if term > currentTerm

And fix: don't forget who we voted for.

But: keep the other bug: neglect to ignore messages if term < currentTerm.
