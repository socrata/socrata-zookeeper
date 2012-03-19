import com.socrata.sbtcommon.SbtCommon._                                                               
                                                                                                       
seq(socrataSettings(): _*)                                                                             
                                                                                                       
name := "socrata-zookeeper"

version := "1.0.0"

scalaVersion := "2.9.1-1"

crossScalaVersions := Seq("2.8.1", "2.9.1-1")

libraryDependencies ++= Seq(
  "com.socrata" %% "socrata-utils" % "1.0.0",
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion, // zoookeeperrrr!
  "org.apache.zookeeper" % "zookeeper" % "3.3.1"
)

