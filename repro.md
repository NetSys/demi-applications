$ git clone -b raft-leader-safety git@github.com:NetSys/sts2-applications.git
$ cd sts2-applications
$ git remote add interposition git@github.com:NetSys/sts2-interposition.git
$ git subtree pull --prefix=interposition interposition master
$ git clone git@github.com:NetSys/sts2-experiments.git experiments
$ sbt assembly
$ java -d64 -Xmx15g -cp target/scala-2.11/randomSearch-assembly-0.1.jar Main 2>&1 | tee console.out