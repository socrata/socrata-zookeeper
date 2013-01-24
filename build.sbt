import SocrataSbtKeys._

seq(socrataSettings(): _*)

name := "socrata-zookeeper"

scalaVersion := "2.10.0"

crossScalaVersions := Seq("2.8.1", "2.9.2", "2.10.0")

libraryDependencies <++= (slf4jVersion) { slf4jVersion =>
  Seq(
    "com.socrata" %% "socrata-utils" % "[0.5.1,1.0.0)",
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion, // zoookeeperrrr!  When 3.5 comes out maybe we can get rid of this...
    "org.apache.zookeeper" % "zookeeper" % "3.4.5"
  )
}

// bllllleargh -- 2.8's doc process blows up thanks to SI-4284
publishArtifact in (Compile, packageDoc) <<= scalaVersion { sv =>
  !sv.startsWith("2.8.")
}
