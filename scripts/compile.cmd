#!/bin/bash -i

export JAVA_HOME=/usr/local/src/jdk1.7.0_07/
export ANT_HOME=/usr/local/src/apache-ant-1.8.4
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

#rm -rf /lucene/util.nightly/build
#mkdir /lucene/util.nightly/build
/usr/bin/python -uO /lucene/util.nightly/src/python/nightlyCompile.py >> /lucene/logs.nightly/compile.log 2>&1
