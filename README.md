# Ingest #

This process requires one additional JAR file and three JSON files.

The extra JAR file can be built directly from the GeoTrellis source tree.
That can be done by typing
```bash
cd $HOME/local/src/geotrellis
./sbt "project spark-etl" assembly
cp spark-etl/target/scala-2.11/geotrellis-spark-etl-assembly-1.0.0-SNAPSHOT.jar /tmp
```
or similar, where paths and the assembly filename are changed as appropriate.

The three required JSON files are in a directory called `json` under the root of this project.
Those three files should be edited as appropriate to make sure that the various paths given therein are correct.
More information about the ETL process can be found [here](https://github.com/geotrellis/geotrellis/blob/master/docs/spark-etl/spark-etl-run-examples.md).

The raw data can be transformed into a GeoTrellis layer with a command similar to either of the following:
```bash
$SPARK_HOME/bin/spark-submit \
   --class geotrellis.spark.etl.SinglebandIngest \
   --master 'local[*]' \
   --driver-memory 16G \
   geotrellis-spark-etl-assembly-1.0.0-SNAPSHOT.jar \
   --input "file:///tmp/input.json" \
   --output "file:///tmp/output.json" \
   --backend-profiles "file:///tmp/backend-profiles.json"
```

# Run #

```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar
```
