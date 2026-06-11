addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.1.1")

// Publishing to Maven Central (sonatype) on tagged releases. Bundles sbt-dynver,
// sbt-pgp, sbt-sonatype and sbt-git, used by the release workflow via `sbt ci-release`.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.9.2")
