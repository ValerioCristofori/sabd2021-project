#!/bin/bash

sudo docker exec -it /usr/bin/create 'query1','cf1'
sudo docker exec -it $1 create 'query2'
sudo docker exec -it $1 create 'query3'