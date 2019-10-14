#!/bin/sh

set -e

: "${JAVA_HOME:=/usr}"
export JAVA_HOME

: "${ZOOKEEPER_QUORUM:=localhost}"
: "${ZOOKEEPER_PORT:=2181}"
sed -e "s/{{ZOOKEEPER_PORT}}/${ZOOKEEPER_PORT}/" \
    -e "s/{{ZOOKEEPER_QUORUM}}/${ZOOKEEPER_QUORUM}/" \
    /hbase/conf/hbase-site.template.xml > /hbase/conf/hbase-site.xml

if [ -t 0 ] || [ "$1" = "shell" ]
then
    # Running interactively so assume we are trying to run an Hbase command
    exec /hbase/bin/hbase "${@}"
fi

pgrep -f proc_rest && pkill -9 -f proc_rest

printf "*** Starting HBase ***"
/hbase/bin/start-hbase.sh

trap_func() {
    printf "*** Shutting down HBase ***"
    /hbase/bin/local-regionservers.sh stop 1 || :
    /hbase/bin/stop-hbase.sh
    sleep 2
    ps -ef | grep org.apache.hadoop.hbase | grep -v -i org.apache.hadoop.hbase.zookeeper | awk '{print $1}' | xargs kill 2>/dev/null || :
    sleep 3
    pkill -f org.apache.hadoop.hbase.zookeeper 2>/dev/null || :
    sleep 2
}
trap trap_func INT QUIT TRAP ABRT TERM EXIT

tail -f /dev/null /hbase/logs/* &

wait || :