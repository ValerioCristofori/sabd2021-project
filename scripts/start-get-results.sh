#!/bin/bash

sudo docker exec -it hdfs-master hadoop fs -mkdir -p /user/hbase/input
sudo docker exec -it hdfs-master /bin/bash -c 'hadoop fs -cat /output/query1/* | hadoop fs -put - /user/hbase/input/query1.csv'
sudo docker exec -it hdfs-master /bin/bash -c 'hadoop fs -cat /output/query2/* | hadoop fs -put - /user/hbase/input/query2.csv'
sudo docker exec -it hdfs-master /bin/bash -c 'hadoop fs -cat /output/query3/* | hadoop fs -put - /user/hbase/input/query3.csv'
sudo docker exec -it hdfs-master /bin/bash -c 'hadoop fs -cat /output/time-queries/* | hadoop fs -put - /user/hbase/input/time-queries.csv'


sudo docker exec -it hdfs-master hadoop fs -copyToLocal /user/hbase/input /hadoop/dfs/
sudo docker cp hdfs-master:/hadoop/dfs/input/. ./Results/
