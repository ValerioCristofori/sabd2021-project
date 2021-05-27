#!/bin/bash

sudo docker kill slave1 slave2 slave3
sudo docker rm master slave1 slave2 slave3

sudo docker network rm hadoop_network