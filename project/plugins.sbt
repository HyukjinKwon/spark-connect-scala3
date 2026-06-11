// Protobuf / gRPC code generation (ScalaPB)
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

// Code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Documentation: compiled, type-checked markdown
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.5.4")

// Publishing to Maven Central (Sonatype) with automatic signing
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.6.1")

// Test coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.12")

// API documentation site (unidoc)
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
