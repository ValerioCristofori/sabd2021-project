#!/bin/bash

sudo docker exec -it $1 hadoop fs -mkdir /merged
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/query1.csv/* | hadoop fs -put - /merged/query1.csv'
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/query2.csv/* | hadoop fs -put - /merged/query2.csv'
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/query3.csv/* | hadoop fs -put - /merged/query3.csv'
sudo docker exec -it $1 /bin/bash -c 'hadoop fs -cat /output/time-queries.csv/* | hadoop fs -put - /merged/time-queries.csv'


sudo docker exec -it $1 hadoop fs -copyToLocal /merged /hadoop/dfs/
sudo docker cp $1:/hadoop/dfs/merged ./results