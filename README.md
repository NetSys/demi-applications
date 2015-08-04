Fix the following bug: step down / update term if term > currentTerm

But: keep the other bug: neglect to ignore messages if term < currentTerm.
