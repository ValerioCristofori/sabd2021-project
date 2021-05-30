FROM docker.io/bitnami/spark:3
USER root

RUN mkdir -p /app
COPY ./target/sabd-project-1.0.jar /app/sabd-project-1.0.jar

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_JAR_LOCATION /app/sabd-project-1.0.jar
ENV SPARK_APPLICATION_MAIN_CLASS main.Main
ENV SPARK_APPLICATION_ARGS ""
