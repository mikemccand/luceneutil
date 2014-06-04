#!/bin/bash -i

# Oracle java 1.7.0_07
#export PATH=/usr/local/src/jdk1.7.0_07/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
#export JAVA_HOME=/usr/local/src/jdk1.7.0_07

# Oracle java 1.7.0_04
#export PATH=/usr/local/src/jdk1.7.0_04/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
#export JAVA_HOME=/usr/local/src/jdk1.7.0_04

# Oracle java 1.7.0_01
#export PATH=/usr/local/src/jdk1.7.0_01/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
#export JAVA_HOME=/usr/local/src/jdk1.7.0_01

# Oracle java 1.7.0_55
export PATH=/usr/local/src/jdk1.7.0_55/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
export JAVA_HOME=/usr/local/src/jdk1.7.0_55

# Oracle java 1.6.0_21
#export PATH=/usr/local/src/jdk1.6.0_21/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
#export JAVA_HOME=/usr/local/src/jdk1.6.0_21

# Oracle java 1.6.0_26
#export PATH=/usr/local/src/jdk1.6.0_26/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
#export JAVA_HOME=/usr/local/src/jdk1.6.0_26

export ANT_HOME=/usr/local/src/apache-ant-1.8.4
export PATH=$ANT_HOME/bin:$PATH

/usr/bin/python -uO /lucene/util.nightly/src/python/nightlyBench.py -run >> /lucene/logs.nightly/all.log 2>&1

/usr/bin/python -uO /lucene/util.nightly/src/python/runAnalyzerPerf.py >> /lucene/logs.nightly/all.log 2>&1

/usr/bin/python -uO /lucene/util.nightly/src/python/sumAnalyzerPerf.py >> /lucene/logs.nightly/all.log 2>&1

