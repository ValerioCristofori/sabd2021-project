#!/bin/bash

sudo docker exec -it $1 hadoop fs -mkdir -p /user/hbase/input
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/query1/* | hadoop fs -put - /user/hbase/input/query1.csv'
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/query2/* | hadoop fs -put - /user/hbase/input/query2.csv'
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/query3/* | hadoop fs -put - /user/hbase/input/query3.csv'
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/time-queries/* | hadoop fs -put - /user/hbase/input/time-queries.csv'


sudo docker exec -it $1 hadoop fs -copyToLocal /user/hbase/input /hadoop/dfs/
sudo docker cp $1:/hadoop/dfs/input/. ./Results/
