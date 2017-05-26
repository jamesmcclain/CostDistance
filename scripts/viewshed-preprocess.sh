#!/usr/bin/env bash

ELEVATION=${1:-boston}
ZOOM=${2:-18}

$SPARK_HOME/bin/spark-submit \
    --master 'local[*]' \
    --driver-memory 16G \
    cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
    'file:///tmp/hdfs-catalog' pyramid $ELEVATION $ZOOM ${ELEVATION}-pyramid 256

$SPARK_HOME/bin/spark-submit \
    --master 'local[*]' \
    --driver-memory 16G \
    cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
    'file:///tmp/hdfs-catalog' histogram $ELEVATION $ZOOM ${ELEVATION}-pyramid 0
