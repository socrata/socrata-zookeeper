import sbt.Keys._

lazy val commonSettings = Seq(
  organization := "com.socrata",
  scalaVersion := "2.11.7",
  scalastyleFailOnError in Compile := false,
  resolvers ++= Seq("socrata maven" at "https://repo.socrata.com/artifactory/libs-release")
)

lazy val socrata_zookeeper = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "socrata-zookeeper",
    crossScalaVersions := Seq("2.10.6", "2.11.7"),
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-utils" % "[0.10.1,1.0.0)",
      "org.slf4j" % "slf4j-log4j12" % "1.7.12", // zoookeeperrrr!  When 3.5 comes out maybe we can get rid of this...
      "org.apache.zookeeper" % "zookeeper" % "3.4.5"
    )
  )

// TODO: enable static analysis build failures
// TODO: Unable to incorporate in common settings....????
com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Compile := false
com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Test := false
