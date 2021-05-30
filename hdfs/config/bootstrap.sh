#!/bin/bash


service ssh start

if [[ -n "${HDFS_WORKERS}" ]]; then
	IFS=',' read -ra WORKERS <<< "${HDFS_WORKERS}"
	for worker in "${WORKERS[@]}"; do
		echo $worker >> $HADOOP_HOME/etc/hadoop/workers;
	done
	echo "Nodi slave configurati"
fi

if [[ "${WHOAMI}" == "master" ]]; then
	hdfs namenode -format; # delete all content of hdfs
	$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
	hdfs dfs -mkdir /input
	hdfs dfs -mkdir /output
	hdfs dfs -chown spark:spark /output
	hdfs dfs -chown nifi:nifi /input
	echo "Nodo master configurato"
fi


while true; do sleep 1000; done

#Wait
wait $!;
