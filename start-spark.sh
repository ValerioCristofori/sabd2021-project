#!/bin/bash

#Inizia ambiente spark su container spark-bitnami

sudo docker network create --driver bridge spark_network

sudo docker run -d --name spark --network=spark_network -e SPARK_MODE=master bitnami/spark

