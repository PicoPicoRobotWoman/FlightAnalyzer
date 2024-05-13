#!/bin/bash

set -e

docker build -t spark-base:latest ./docker-cluster/docker-images/base
docker build -t spark-master:latest ./docker-cluster/docker-images/spark-master
docker build -t spark-worker:latest ./docker-cluster/docker-images/spark-worker
docker build -t spark-submit:latest ./docker-cluster/docker-images/spark-submit

echo "Сборка Docker-образов завершена."

sbt assembly

echo "сборка spark job (.jar) завершина"

cp ./target/scala-2.12/FlightAnalyzer.1.0.0.jar ./docker-cluster/spark-apps/job.jar

cd docker-cluster

docker-compose up -d --scale spark-worker=3

docker exec -it docker-cluster-spark-master-1 bin/bash -c '

# shellcheck disable=SC1004
 ./spark/bin/spark-submit \
 --class com.example.FlightAnalyzer \
 --deploy-mode client \
 --master spark://docker-cluster-spark-master-1:7077 \
 --verbose \
 --supervise /opt/spark-apps/job.jar

'