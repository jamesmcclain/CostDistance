name := "cdistance"
libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-geotools"  % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-raster"    % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-spark"     % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vector"    % "1.2.0-SNAPSHOT",
  "org.apache.hadoop"            % "hadoop-client"        % Version.hadoop % "provided",
  "org.apache.spark"            %% "spark-core"           % Version.spark  % "provided"
)

fork in Test := false
parallelExecution in Test := false
