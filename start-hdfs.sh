#!/bin/bash

	#init env  -> Start Dockerfile
#sudo docker build -t effeerre/hadoop ./docker/hdfs/

#We can now create an isolated network with 3 datanodes and a namenode

sudo docker network create --driver bridge hadoop_network
sudo docker run -t -i -p 9864:9864 -d --network=hadoop_network --name=slave1 effeerre/hadoop
sudo docker run -t -i -p 9863:9864 -d --network=hadoop_network --name=slave2 effeerre/hadoop
sudo docker run -t -i -p 9862:9864 -d --network=hadoop_network --name=slave3 effeerre/hadoop
sudo docker run -t -i -p 9870:9870 --network=hadoop_network --name=master effeerre/hadoop

