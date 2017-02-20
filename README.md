# Build #

Type
```bash
sbt "project cdistance" assembly
sbt "project server" assembly
```
to build the pre-processing jar and the TMS server jar.

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

The raw data can be transformed into a GeoTrellis layer with a command similar to the following:
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

# Pre-Process #

## Local ##

The pre-processing step can be done manually or with the provided `preprocess.sh` script.

### Manual ###

Compute the `slope` layer to use as the friction layer
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' slope ned slope <z>
```
where `<z>` is replaced by the appropriate zoom level.

Compute the `cost` layer:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' costdistance slope cost <z> /tmp/cities-3857/cities-3857.shp 20000
```

Compute the histogram of the `cost` layer:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' histogram cost <z>
```

Pyramid the `cost` layer into `cost-pyramid`:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.jar \
   'file:///tmp/hdfs-catalog' pyramid cost <z> cost-pyramid 256
```

### Script ###

Simply type
```bash
./preprocess.sh
```
to perform all of the above steps.
The script also takes the name of the elevation layer and its zoom as optional arguments.
The lines
```bash
./preprocess.sh ned
```
and
```bash
./preprocess.sh ned 9
```
are examples.

## EMR ##

The `preprocess.sh` script is not suitable for use on EMR,
because it lacks command line arguments to `spark-submit` that are necessary to make it run smoothly.
Either modify the script so that the invocations of `spark-submit` resemble the one below, or do the pre-processing steps manually.
Please note: the invocation below assumes the existance of the `log4j.properties` file (copied from the `resources` directory of the pre-processing jar) on the EMR master.
Also: depending on the size dataset, some amount of trial-and-error may be required to get a set of working `spark-submit` arguments.

On EMR, the `cost` layer can be computed in something like the following way.
```bash
spark-submit \
   --master yarn \
   --driver-memory 12G \
   --conf "spark.yarn.executor.memoryOverhead=6G" \
   --conf "spark.driver.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p' -Dlog4j.configuration=file:///home/hadoop/log4j.properties" \
   cdistance-assembly-0.jar \
   'hdfs:/catalog' costdistance slope cost <z> /tmp/cities-3857/cities-3857.shp 20000
```

# Demo Server #

## Local ##

Start the TMS server:
```bash
$SPARK_HOME/bin/spark-submit server/target/scala-2.11/server-assembly-0.jar
```

## EMR ##

Modifiations the `application.conf` will be required.
