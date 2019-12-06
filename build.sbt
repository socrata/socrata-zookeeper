organization := "com.socrata"

name := "socrata-zookeeper"

resolvers ++= Seq("socrata maven" at "https://repo.socrata.com/artifactory/libs-release")

scalaVersion := "2.12.8"

crossScalaVersions := Seq("2.10.6", "2.11.7", scalaVersion.value)

mimaPreviousArtifacts := Set("com.socrata" %% "socrata-zookeeper" % "1.1.0")

libraryDependencies ++= Seq(
  "com.socrata" %% "socrata-utils" % "0.11.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.12", // zoookeeperrrr!  When 3.5 comes out maybe we can get rid of this...
  "org.apache.zookeeper" % "zookeeper" % "3.4.5"
)

