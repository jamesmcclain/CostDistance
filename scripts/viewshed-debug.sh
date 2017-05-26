#!/usr/bin/env bash

VIEWSHED=${1:-moop}
ELEVATION=ned
ZOOM=9
MAXDISTANCE=400000

mkdir -p /tmp/tif/
rm -rf /tmp/hdfs-catalog/${VIEWSHED}
rm -rf /tmp/tif/${VIEWSHED}*.tif

$SPARK_HOME/bin/spark-submit \
    --master 'local[*]' \
    --driver-memory 16G \
    cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
    'file:///tmp/hdfs-catalog' viewshed ${ELEVATION} ${VIEWSHED} ${ZOOM} ${MAXDISTANCE} OR -10877415 4448309 4000 0 -1 0

$SPARK_HOME/bin/spark-submit \
    --master 'local[*]' \
    --driver-memory 16G \
    cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
    'file:///tmp/hdfs-catalog' dump ${VIEWSHED} ${ZOOM}

rm -f /tmp/viewshed.tif*
gdal_merge.py /tmp/tif/${VIEWSHED}*.tif -o /tmp/viewshed.tif
