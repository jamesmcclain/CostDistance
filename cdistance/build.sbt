name := "cdistance"
libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-accumulo"  % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-geotools"  % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-raster"    % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-spark"     % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vector"    % "1.2.0-SNAPSHOT",
  "org.geotools"                 % "gt-geotiff"           % Version.geotools,
  "javax.media"                  % "jai_core"             % "1.1.3" from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar",
  "org.apache.hadoop"            % "hadoop-client"        % Version.hadoop % "provided",
  "org.apache.spark"            %% "spark-core"           % Version.spark  % "provided"
)

fork in Test := false
parallelExecution in Test := false
