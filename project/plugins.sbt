logLevel := Level.Warn

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "3.0.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

addSbtPlugin("de.johoop" % "findbugs4sbt" % "1.4.0")

addSbtPlugin("de.johoop" % "cpd4sbt" % "1.1.5")

addSbtPlugin("de.corux" %% "sbt-code-quality" % "0.2.0")

resolvers += "corux-releases" at "http://tomcat.corux.de/nexus/content/repositories/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

