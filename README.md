# Raw Data #

The demo uses [Mapzen Elevation Data](https://mapzen.com/blog/elevation) covering roughly the continental U.S.
Those data can be obtained by executing the following command in the directory `/tmp/NED`.
```bash
for x in $(seq 19 36)
do
  for y in $(seq 43 54)
  do
    wget https://terrain-preview.mapzen.com/geotiff/7/${x}/${y}.tif -O 7_${x}_${y}.tif
  done
done
```

# Build Jars #

Type
```bash
sbt "project cdistance" assembly
sbt "project server" assembly
```
to build the pre-processing jar and the TMS server jar.

# Ingest #

This process requires one additional JAR file.
The extra JAR file can be built directly from the GeoTrellis source tree by typing
```bash
cd $HOME/local/src/geotrellis
./sbt "project spark-etl" assembly
cp spark-etl/target/scala-2.11/geotrellis-spark-etl-assembly-1.0.0-SNAPSHOT.jar /tmp
```
or similar (where paths and the assembly filename are changed appropriatly).

The raw data can be transformed into a GeoTrellis layer with a command similar to the following
```bash
$SPARK_HOME/bin/spark-submit \
   --class geotrellis.spark.etl.SinglebandIngest \
   --master 'local[*]' \
   --driver-memory 16G \
   geotrellis-spark-etl-assembly-1.0.0-SNAPSHOT.jar \
   --input "file://$(pwd)/json/input.json" \
   --output "file://$(pwd)/json/output.json" \
   --backend-profiles "file://$(pwd)/json/backend-profiles.json"
```
or by typing
```bash
./scripts/ingest.sh
```
to use the provided ingest script.

Please note that the ingest script is only suitable for local use
due to the options used to invoke `spark-submit`,
and because of the assumed source and destination locations of the data.

# Pre-Process #

## Local ##

The pre-processing step can be done manually or with the provided `preprocess.sh` script.

### Manual ###

Compute the `slope` layer to use as the friction layer
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
   'file:///tmp/hdfs-catalog' slope ned slope <z>
```
where `<z>` is replaced by the appropriate zoom level.

Compute the `cost` layer:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
   'file:///tmp/hdfs-catalog' costdistance slope cost <z> /tmp/cities-3857/cities-3857.shp 20000
```

Compute the histogram of the `cost` layer:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
   'file:///tmp/hdfs-catalog' histogram cost <z>
```

Pyramid the `cost` layer into `cost-pyramid`:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
   'file:///tmp/hdfs-catalog' pyramid cost <z> cost-pyramid 256
```

### Script ###

Simply type
```bash
./scripts/preprocess.sh
```
to perform all of the above steps.
The script also takes the name of the elevation layer and its zoom as optional arguments.

## EMR ##

The `preprocess.sh` script is not suitable for use on EMR
because the command line arguments to `spark-submit` in the script are not sufficient to make it run smoothly.
Either modify the script so that the invocations of `spark-submit` resemble the one below,
or do the pre-processing steps manually.
Please note that the invocation below assumes the existance of the `log4j.properties` file
(copied from the `resources` directory of the pre-processing jar)
on the EMR master.
Depending on the size dataset,
some amount of trial-and-error may be required to get a set of working `spark-submit` arguments.

On EMR, the `cost` layer can be computed in something like the following way.
```bash
spark-submit \
   --master yarn \
   --driver-memory 12G \
   --conf "spark.yarn.executor.memoryOverhead=6G" \
   --conf "spark.driver.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p' -Dlog4j.configuration=file:///home/hadoop/log4j.properties" \
   cdistance-assembly-0.22.7.jar \
   'hdfs:/catalog' costdistance slope cost <z> /tmp/cities-3857/cities-3857.shp 20000
```

# Demo Server #

## Local ##

Start the TMS server by typing
```bash
$SPARK_HOME/bin/spark-submit server/target/scala-2.11/server-assembly-0.jar
```
or
```bash
./scripts/server.sh
```

## EMR ##

Modifiations the `application.conf` will be required.
After that, type
```bash
spark-submit server-assembly-0.jar
```
or similar.

# Other Commands #

The "copy" command copies a layer:
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
   'file:///tmp/hdfs-catalog' copy ned ned-copy 9
```

The "mask" command masks against a GeoJSON polygon.
The layer is assumed to be in the WebMercator projection.
```bash
$SPARK_HOME/bin/spark-submit \
   --master 'local[*]' \
   --driver-memory 16G \
   cdistance/target/scala-2.11/cdistance-assembly-0.22.7.jar \
   'file:///tmp/hdfs-catalog' mask ned ned-masked 9 ./geojson/USA.geo.json
```
