#!/bin/bash

sudo docker exec -it $1  /opt/bitnami/spark/bin/spark-submit --class main.Main --master "local" /app/sabd-project-1.0.jar
