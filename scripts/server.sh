#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master='local[*]' \
    --conf "spark.driver.maxResultSize=0" \
    --driver-memory 16G \
    server/target/scala-2.11/server-assembly-0.22.7.jar
