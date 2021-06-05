#!/bin/bash

# Start spark script
./scripts/start-spark-submit.sh

# Start hdfs script for getting data
./scripts/start-get-results.sh

# Start hbase script for create tables
./scripts/create-tables.sh