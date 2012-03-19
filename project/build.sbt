resolvers := Seq("socrata maven" at ("http://" + System.getProperty("socrata.maven.host", "nexus.socrata.com") + "/nexus/content/groups/public/"))

externalResolvers <<= resolvers map { rs =>
  Resolver.withDefaultResolvers(rs, mavenCentral = false, scalaTools = false)
}

addSbtPlugin("com.socrata" % "sbt-common" % "1.0.0")
