name := "server"
libraryDependencies ++= Seq(
  "com.google.guava"             % "guava"            % "16.0.1",
  "com.typesafe.akka"           %% "akka-actor"       % "2.4.14",
  "io.spray"                    %% "spray-can"        % "1.3.3",
  "io.spray"                    %% "spray-routing"    % "1.3.3",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.0.0-SNAPSHOT",
  "org.apache.hadoop"            % "hadoop-client"    % Version.hadoop % "provided",
  "org.apache.spark"            %% "spark-core"       % Version.spark  % "provided"
).map({ _ exclude("com.google.guava", "guava")})

fork in Test := false
parallelExecution in Test := false
