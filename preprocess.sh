#!/usr/bin/env bash

ELEVATION=${1:-ned}
ZOOM=${2:-9}

$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' slope $ELEVATION slope $ZOOM

$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' costdistance slope cost $ZOOM /tmp/cities-3857/cities-3857.shp 20000

$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' histogram cost $ZOOM

$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' pyramid cost $ZOOM cost-pyramid 256
