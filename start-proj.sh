#!/bin/bash

# Script per automatizzare il processo di run del progetto

$SPARK_HOME/sbin/start-master.sh

# Starts a slave instance on each machine specified #   in the conf/slaves file on the master node
$SPARK_HOME/sbin/start-slave.sh 

# Starts spark application
$SPARK_HOME/bin/spark-submit --class main.Main --master "local" target/sabd-project-1.0.jar
