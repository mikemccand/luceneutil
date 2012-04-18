#!/bin/bash -i

# Oracle java 1.6.0_21
export PATH=/usr/local/src/jdk1.6.0_21/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
export JAVA_HOME=/usr/local/src/jdk1.6.0_21

/usr/bin/python -uO /lucene/util.nightly/nightlyCompile.py >> /lucene/logs.nightly/compile.log 2>&1

