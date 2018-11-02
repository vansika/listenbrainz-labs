#!/bin/bash

. /root/hadoop-env.sh

# starting SSH
echo "start SSH"
service ssh start

echo "start HDFS"
$HADOOP_HOME/sbin/start-dfs.sh

echo "start Yarn"
$HADOOP_HOME/sbin/start-yarn.sh

while :; do sleep 1000; done
