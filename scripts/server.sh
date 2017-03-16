#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit --master='local[*]' --driver-memory 16G server/target/scala-2.11/server-assembly-0.jar
