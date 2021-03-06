import sbtassembly.PathList

val generalDeps = Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" exclude("javax.servlet", "servlet-api")
)

val extraResolvers = Seq(
  Resolver.mavenLocal,
  "osgeo" at "http://download.osgeo.org/webdav/geotools/"
)

lazy val commonSettings = Seq(
  organization := "com.example",
  version := "0.22.7",
  scalaVersion := "2.11.11",
  test in assembly := {},
  scalacOptions ++= Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-language:experimental.macros",
    "-feature"),
  assemblyMergeStrategy in assembly := {
    case "log4j.properties" => MergeStrategy.first
    case "reference.conf" => MergeStrategy.concat
    case "application.conf" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) =>
      xs match {
        case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
        // Concatenate everything in the services directory to keep GeoTools happy.
        case ("services" :: _ :: Nil) =>
          MergeStrategy.concat
        // Concatenate these to keep JAI happy.
        case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
          MergeStrategy.concat
        case (name :: Nil) => {
          // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
          if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
            MergeStrategy.discard
          else
            MergeStrategy.first
        }
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  },
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= generalDeps)

lazy val cdistance = (project in file("cdistance"))
  .dependsOn(root)
  .settings(commonSettings: _*)
  .settings(resolvers ++= extraResolvers)

lazy val server = (project in file("server"))
  .dependsOn(root)
  .settings(commonSettings: _*)
