#!/bin/bash


cleanup() {
	if [[ "${HDFS_MODE}" == "master" ]]; then 
		$HADOOP_HDFS_HOME/sbin/stop-dfs.sh;
	fi
}

#Trap SIGTERM
trap 'cleanup' SIGTERM;



if [[ -n "${HDFS_WORKERS}" ]]; then
	IFS=',' read -ra WORKERS <<< "${HDFS_WORKERS}"
	for worker in "${WORKERS[@]}"; do
		echo $worker >> $HADOOP_HOME/etc/hadoop/workers;
	done
fi

service ssh start
# $HADOOP_PREFIX/sbin/start-dfs.sh
if [[ -n "${HDFS_WORKERS}" ]]; then
	IFS=',' read -ra WORKERS <<< "${HDFS_WORKERS}"
	for worker in "${WORKERS[@]}"; do
		echo $worker >> $HADOOP_HOME/etc/hadoop/workers;
	done
fi

if [[ "${HDFS_MODE}" == "master" ]]; then
	echo "Start nodo master";
	hdfs namenode -format; # delete all content of hdfs
	$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
	hdfs dfs -mkdir input
	hdfs dfs -mkdir output
	hdfs dfs -cp ../../data input/
	echo "Nodo master configurato"
fi

while true; do sleep 1000; done

#Wait
wait $!;