#!/bin/bash -i

export JAVA_HOME=/usr/local/src/jdk1.8.0_101/
export ANT_HOME=/usr/local/src/apache-ant-1.9.5/
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

rm -rf /l/util.nightly/build
mkdir /l/util.nightly/build
#/usr/bin/python -uO /lucene/util.nightly/src/python/nightlyCompile.py >> /l/logs.nightly/compile.log 2>&1
/usr/bin/python -uO /l/util.nightly/src/python/nightlyCompile.py
