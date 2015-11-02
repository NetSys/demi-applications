Test applications for DEMi
=================

This repository contains example applications tested with DEMi.
For documentation on how to test your system with DEMi, see this page.

Each branch contains one case study. Master is currently set up to
allow one to use the interposition code relatively quickly. interposition itself is pulled in using git subtree. For
building an application, just branch from master and write code. To update the interposition code itself:

```
git remote add interposition git@github.com:NetSys/demi.git
git subtree pull --prefix=interposition interposition master
```

sbt and most of the other goodies are set up correctly.

An unfortunate side-effect of this setup is that there are often merge conflicts when you push to the subtree on one branch and pull in the changes to the other. The way I workaround this is to run the following for each file with a merge conflict:

```
$ git checkout --theirs $CONFLICT_FILE
$ git add $CONFLICT_FILE
```

#### Branches:

Branch | Application
-------|------------
akka-raft | mainline akka-raft instrumentation
colin-rbcast | fully-fleshed out reliable broadcast application
panda-rbcast | One of three reliable broadcast applications
raft-* | Case studies for issues on the akka-raft repo. https://github.com/ktoso/akka-raft/issues
raft-leader-safety | deprecated. https://github.com/ktoso/akka-raft/issues/47
raft-shrunk-leader-safety | deprecated. https://github.com/ktoso/akka-raft/issues/47
raft-strange-cluster-membership | https://github.com/ktoso/akka-raft/issues/49
spark | mainline spark instrumentation
spark-* | Case studies for issues on the Spark repo. https://issues.apache.org/jira/browse/spark/
test-ask | an integration test for our handling of Akka's `ask'

#### Prequisites

```
# Install scala
$ sudo apt-get install scala
# Install sbt
$ cd /tmp
$ wget "https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb"
$ sudo dpkg -i sbt-0.13.7.deb
```
