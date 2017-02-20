#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
   --class geotrellis.spark.etl.SinglebandIngest \
   --master 'local[*]' \
   --driver-memory 16G \
   geotrellis-spark-etl-assembly-1.0.0-SNAPSHOT.jar \
   --input "file://$(pwd)/json/input.json" \
   --output "file://$(pwd)/json/output.json" \
   --backend-profiles "file://$(pwd)/json/backend-profiles.json"
