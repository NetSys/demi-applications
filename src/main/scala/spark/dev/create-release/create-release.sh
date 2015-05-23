#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Quick-and-dirty automation of making maven and binary releases. Not robust at all.
# Publishes releases to Maven and packages/copies binary release artifacts.
# Expects to be run in a totally empty directory.
#
# Options:
#  --package-only   only packages an existing release candidate
#
# Would be nice to add:
#  - Send output to stderr and have useful logging in stdout

GIT_USERNAME=${GIT_USERNAME:-pwendell}
GIT_PASSWORD=${GIT_PASSWORD:-XXX}
GPG_PASSPHRASE=${GPG_PASSPHRASE:-XXX}
GIT_BRANCH=${GIT_BRANCH:-branch-1.0}
RELEASE_VERSION=${RELEASE_VERSION:-1.0.0}
RC_NAME=${RC_NAME:-rc2}
USER_NAME=${USER_NAME:-pwendell}

set -e

GIT_TAG=v$RELEASE_VERSION-$RC_NAME

if [[ ! "$@" =~ --package-only ]]; then
  echo "Creating and publishing release"
  # Artifact publishing
  git clone https://git-wip-us.apache.org/repos/asf/spark.git -b $GIT_BRANCH
  cd spark
  export MAVEN_OPTS="-Xmx3g -XX:MaxPermSize=1g -XX:ReservedCodeCacheSize=1g"

  mvn -Pyarn release:clean

  mvn -DskipTests \
    -Darguments="-DskipTests=true -Dmaven.javadoc.skip=true -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 -Dgpg.passphrase=${GPG_PASSPHRASE}" \
    -Dusername=$GIT_USERNAME -Dpassword=$GIT_PASSWORD \
    -Dmaven.javadoc.skip=true \
    -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 \
    -Pyarn -Phive -Phadoop-2.2 -Pspark-ganglia-lgpl\
    -Dtag=$GIT_TAG -DautoVersionSubmodules=true \
    --batch-mode release:prepare

  mvn -DskipTests \
    -Darguments="-DskipTests=true -Dmaven.javadoc.skip=true -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 -Dgpg.passphrase=${GPG_PASSPHRASE}" \
    -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 \
    -Dmaven.javadoc.skip=true \
    -Pyarn -Phive -Phadoop-2.2 -Pspark-ganglia-lgpl\
    release:perform

  cd ..
  rm -rf spark
fi

# Source and binary tarballs
echo "Packaging release tarballs"
git clone https://git-wip-us.apache.org/repos/asf/spark.git
cd spark
git checkout --force $GIT_TAG
release_hash=`git rev-parse HEAD`

rm .gitignore
rm -rf .git
cd ..

cp -r spark spark-$RELEASE_VERSION
tar cvzf spark-$RELEASE_VERSION.tgz spark-$RELEASE_VERSION
echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --armour --output spark-$RELEASE_VERSION.tgz.asc \
  --detach-sig spark-$RELEASE_VERSION.tgz
echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md MD5 spark-$RELEASE_VERSION.tgz > \
  spark-$RELEASE_VERSION.tgz.md5
echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md SHA512 spark-$RELEASE_VERSION.tgz > \
  spark-$RELEASE_VERSION.tgz.sha
rm -rf spark-$RELEASE_VERSION

make_binary_release() {
  NAME=$1
  FLAGS=$2
  cp -r spark spark-$RELEASE_VERSION-bin-$NAME
  
  cd spark-$RELEASE_VERSION-bin-$NAME
  ./make-distribution.sh $FLAGS --name $NAME --tgz
  cd ..
  cp spark-$RELEASE_VERSION-bin-$NAME/spark-$RELEASE_VERSION-bin-$NAME.tgz .
  rm -rf spark-$RELEASE_VERSION-bin-$NAME

  echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --armour \
    --output spark-$RELEASE_VERSION-bin-$NAME.tgz.asc \
    --detach-sig spark-$RELEASE_VERSION-bin-$NAME.tgz
  echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md \
    MD5 spark-$RELEASE_VERSION-bin-$NAME.tgz > \
    spark-$RELEASE_VERSION-bin-$NAME.tgz.md5
  echo $GPG_PASSPHRASE | gpg --passphrase-fd 0 --print-md \
    SHA512 spark-$RELEASE_VERSION-bin-$NAME.tgz > \
    spark-$RELEASE_VERSION-bin-$NAME.tgz.sha
}

make_binary_release "hadoop1" "--with-hive --hadoop 1.0.4"
make_binary_release "cdh4" "--with-hive --hadoop 2.0.0-mr1-cdh4.2.0"
make_binary_release "hadoop2" "--with-hive --with-yarn --hadoop 2.2.0"

# Copy data
echo "Copying release tarballs"
rc_folder=spark-$RELEASE_VERSION-$RC_NAME
ssh $USER_NAME@people.apache.org \
  mkdir /home/$USER_NAME/public_html/$rc_folder
scp spark-* \
  $USER_NAME@people.apache.org:/home/$USER_NAME/public_html/$rc_folder/

# Docs
cd spark
sbt/sbt clean
cd docs
PRODUCTION=1 jekyll build
echo "Copying release documentation"
rc_docs_folder=${rc_folder}-docs
ssh $USER_NAME@people.apache.org \
  mkdir /home/$USER_NAME/public_html/$rc_docs_folder
rsync -r _site/* $USER_NAME@people.apache.org:/home/$USER_NAME/public_html/$rc_docs_folder

echo "Release $RELEASE_VERSION completed:"
echo "Git tag:\t $GIT_TAG"
echo "Release commit:\t $release_hash"
echo "Binary location:\t http://people.apache.org/~$USER_NAME/$rc_folder"
echo "Doc location:\t http://people.apache.org/~$USER_NAME/$rc_docs_folder"
