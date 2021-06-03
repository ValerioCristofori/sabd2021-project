#!/bin/bash

sudo docker exec -i $1 hbase shell << EOF
create 'query1','cf1'
create 'query2','cf2'
create 'query3','cf3'
list
EOF

sudo docker exec -i $1 /bin/bash -c 'hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="," -Dimporttsv.columns="HBASE_ROW_KEY,cf1:media" query1 hdfs://hdfs-master:54310/user/hbase/input/query1.csv'
sudo docker exec -i $1 /bin/bash -c 'hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="," -Dimporttsv.columns="HBASE_ROW_KEY,cf2:vaccinazioni_predette" query2 hdfs://hdfs-master:54310/user/hbase/input/query2.csv'
sudo docker exec -i $1 /bin/bash -c 'hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="," -Dimporttsv.columns="HBASE_ROW_KEY,cf3:costo,cf3:wssse,cf3:percentuale,cf3:k_predetto" query3 hdfs://hdfs-master:54310/user/hbase/input/query3.csv'
sudo docker exec -i $1 hbase shell