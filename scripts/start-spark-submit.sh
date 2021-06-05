#!/bin/bash

sudo docker exec -it spark /opt/bitnami/spark/bin/spark-submit --class main.Main --master "local" /app/sabd-project-1.0.jar
