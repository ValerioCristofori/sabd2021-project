#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

service ssh start

#if [[ "${HDFS_MODE}" == "master" ]]; then
#	echo "Start nodo master";
#	hdfs namenode -format; # delete all content of hdfs
#	$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
#	hdfs dfs -mkdir input
#	hdfs dfs -mkdir output
#	hdfs dfs -cp ../../data input/
#	hdfs dfs -chown spark:spark /output
#	echo "Nodo master configurato"
#fi

hdfs namenode -format; # delete all content of hdfs
$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
hdfs dfs -mkdir input
hdfs dfs -mkdir output
hdfs dfs -chown spark:spark /output


/bin/bash
