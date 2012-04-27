import SocrataSbtKeys._

seq(socrataSettings(): _*)

name := "socrata-zookeeper"

scalaVersion := "2.9.2"

crossScalaVersions := Seq("2.8.1", "2.9.2")

libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
  Seq(
    "com.socrata" %% "socrata-utils" % "0.0.1",
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion, // zoookeeperrrr!
    "org.apache.zookeeper" % "zookeeper" % "3.3.1"
  )
}
