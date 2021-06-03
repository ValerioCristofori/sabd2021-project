#!/bin/bash

# Start spark script
./scripts/start-spark-submit.sh $1

# Start hdfs script for getting data
./scripts/start-get-results.sh $2

# Start hbase script for create tables
./scripts/create-tables.sh $3