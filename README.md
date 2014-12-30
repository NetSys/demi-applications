Test applications for STS2
=================

This is mostly meant to be a repository containing test applications, one per branch. Master is currently set up to 
allow one to use the interposition code relatively quickly. interposition itself is pulled in using git subtree. For 
building an application, just branch from master and write code. To update the interposition code itself:

```
git remote add interposition git@github.com:NetSys/sts2-interposition.git
git subtree pull --prefix=interposition interposition master
```

sbt and most of the other goodies are set up correctly

Branches:

Branch | Application
-------|------------
panda-rbcast | One of three reliable broadcast applications
colin-rbcast | fully-fleshed out reliable broadcast application


#### Prequisites

```
# Install scala
$ sudo apt-get install scala
# Install sbt
$ cd /tmp
$ wget "https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb"
$ sudo dpkg -i sbt-0.13.7.deb
```
