#!/bin/bash -i

# disable bash leniency, and log all errors to stderr!
set -ex

# Oracle java 1.6.0_26
#export PATH=/usr/local/src/jdk1.6.0_26/bin:/usr/lib64/qt-3.3/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/lib64/ccache:/usr/local/bin:/bin:/usr/bin
#export JAVA_HOME=/usr/local/src/jdk1.6.0_26

# Oracle java 11
# export ANT_HOME=/usr/local/src/apache-ant-1.9.5
# export JAVA_HOME=/opt/jdk-11.0.2/
# export GRADLE_HOME=/usr/local/src/gradle-2.9
# export PATH=/l/rally:$ANT_HOME/bin:$JAVA_HOME/bin:$GRADLE_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

# OpenJDK java 13
# export ANT_HOME=/usr/local/src/apache-ant-1.9.5
# export JAVA_HOME=/opt/jdk-13.0.1/
# export GRADLE_HOME=/usr/local/src/gradle-2.9
# export PATH=/l/rally:$ANT_HOME/bin:$JAVA_HOME/bin:$GRADLE_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

# OpenJDK java 12
# export ANT_HOME=/usr/local/src/apache-ant-1.9.5
# export JAVA_HOME=/opt/jdk-12.0.2
# export GRADLE_HOME=/usr/local/src/gradle-2.9
# export PATH=/l/rally:$ANT_HOME/bin:$JAVA_HOME/bin:$GRADLE_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

# OpenJDK java 15
#export ANT_HOME=/usr/local/src/apache-ant-1.9.5
#export JAVA_HOME=/opt/jdk-15.0.1/
#export GRADLE_HOME=/usr/local/src/gradle-2.9
#export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$GRADLE_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

# OpenJDK java 17
export ANT_HOME=/usr/local/src/apache-ant-1.9.5
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export GRADLE_HOME=/usr/local/src/gradle-2.9
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$GRADLE_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games

echo -e "\n\n$(date): now start nightly benchmarks"

pkill python3 || true
pkill python || true
pkill java || true
pkill mencoder || true

cd /l/util.nightly

# Force clean compile
rm -rf build || true
mkdir build

git pull origin master

# Do this one first so the rsync run in nightlyBench.py picks up the chart:
echo "Run gradle test"
/usr/bin/python3 -uO /l/util.nightly/src/python/runNightlyGradleTestPrecommit.py >> /l/logs.nightly/all.log 2>&1

echo "Run lucene nightly"
/usr/bin/python -uO /l/util.nightly/src/python/nightlyBench.py -run >> /l/logs.nightly/all.log 2>&1

/usr/bin/python -uO /l/util.nightly/src/python/runAnalyzerPerf.py >> /l/logs.nightly/all.log 2>&1

/usr/bin/python -uO /l/util.nightly/src/python/sumAnalyzerPerf.py >> /l/logs.nightly/all.log 2>&1

echo
echo "Run geo benches"
cd /l/trunk.nightly/lucene
git checkout main
git pull origin main
../gradlew clean
../gradlew jar >> gradlew.jar.geo.log 2>&1
rm -rf /b/osm?.*.small
/usr/bin/python3 /l/util.nightly/src/python/runGeoBenches.py -nightly -reindex
/usr/bin/python3 /l/util.nightly/src/python/writeGeoGraphs.py

echo
echo "Run sparse NYC benches"
cd /l/util.nightly
/usr/bin/python3 -u src/python/sparsetaxis/nightly.py

#/usr/bin/python3 -u systemtests/benchmark.py writeGraph -copy >> /l/logs.nightly/all.log 2>&1

echo "Now rsync"
date
rsync --delete -lrtSO /l/logs.nightly/ /x/tmp/beast3.logs/logs.nightly/

echo "Touch nightly marker file..."
date

touch /x/tmp/beast3.nightly.bench

echo "Copy all.log up..."
/usr/bin/python3 -u src/python/syncAllLog.py

echo "Backup bench logs to /ssd/logs.nightly.bak"
rsync -a /l/logs.nightly/ /ssd/logs.nightly.bak/
