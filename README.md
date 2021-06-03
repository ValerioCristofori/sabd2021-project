# SABD2021-project
Analysis of the dataset of anti Covid-19 vaccinations with Hadoop / Spark

For the purposes of this project, is used the dataset available at the URL 
https://github.com/italia/covid19-opendata-vaccini/tree/master/dati


## Query1
Using punti-somministrazione-tipologia.csv and somministrazioni-vaccini-summary-latest.csv: for each calendar month and for each region, calculate the average number of vaccinations that has been carried out daily in a generic vaccination centerin that region and during that month

## Query2
Using somministrazioni-vaccini-latest.csv: only for women, for each category and each calendar month, determine the top-5 regions for which the highest number of vaccinations is expected on the first day of the following month. To determine the monthly ranking and predict the number of vaccinations, consider the regression line that approximates the trend of daily vaccinations

## Query3
Using somministrazioni-vaccini-summary-latest.csv: predict the total number of administrations carried out as of June 1, 2021 starting from December 27, 2020 considering all the categories and, using a clustering algorithm, classify the areas into Kclusters considering for each area the estimate of the percentage of vaccinated population
–Clustering algorithms:
	•K-means 
	•Bisecting K-means (Spark)
Compare the quality of the clustering solution and the performance of the two algorithms when Kvaries from 2 to 5


# Installation and Usage

Clone the repository and in the directory of Makefile

```
sudo make build
sudo make up
sudo make app
```

For building the .jar and setup the docker-compose env


## Interfaces

* Hdfs: http://localhost:9870
* Spark: http://localhost:8080
* Nifi: http://localhost:8081
* HBase: http://localhost:8085

Check that nifi has downloaded and uploaded the files to hdfs and run

```
docker ps
```

Take the containers id of spark, hdfs-master and hbase and run on the same directory

```
./start-proj.sh ${spark-id} ${hdfs-master-id} ${hbase-id}
```

You can run after processing

```
scan 'query1'
scan 'query2'
scan 'query3'
```
to see the results of the queries saved in hbase