import scalapb.compiler.Version.{grpcJavaVersion, scalapbVersion}

// ---------------------------------------------------------------------------
// Global build settings
// ---------------------------------------------------------------------------

ThisBuild / scalaVersion := "3.3.4" // Scala 3 LTS
ThisBuild / organization := "io.github.hyukjinkwon"
ThisBuild / organizationName := "Hyukjin Kwon"
ThisBuild / homepage := Some(url("https://github.com/HyukjinKwon/spark-connect-scala3"))
ThisBuild / licenses := Seq(
  "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")
)
ThisBuild / developers := List(
  Developer(
    id = "HyukjinKwon",
    name = "Hyukjin Kwon",
    email = "gurwls223@apache.org",
    url = url("https://github.com/HyukjinKwon")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/HyukjinKwon/spark-connect-scala3"),
    "scm:git:git@github.com:HyukjinKwon/spark-connect-scala3.git"
  )
)
ThisBuild / versionScheme := Some("early-semver")

// sbt-ci-release publishes to Maven Central (Sonatype Central). Snapshots
// publish to the snapshots repo; tags (vX.Y.Z) publish releases.
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"

val arrowVersion = "17.0.0"
val munitVersion = "1.0.2"
val slf4jVersion = "2.0.13"

// Arrow on JDK 17 needs these module opens for off-heap memory access.
val arrowJvmOpts = Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-encoding",
    "utf8",
    "-Wunused:imports",
    "-language:implicitConversions"
  ),
  Test / fork := true,
  Test / parallelExecution := false,
  Test / javaOptions ++= arrowJvmOpts,
  run / javaOptions ++= arrowJvmOpts,
  run / fork := true
)

// ---------------------------------------------------------------------------
// Modules
// ---------------------------------------------------------------------------

// proto: ScalaPB-generated message + gRPC stubs from the vendored Spark Connect
// protocol definitions. Pinned to the Apache Spark 4.1.0 (Spark Connect 4.1) protocol.
lazy val proto = (project in file("modules/proto"))
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3-proto",
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
      "io.grpc" % "grpc-netty" % grpcJavaVersion,
      "io.grpc" % "grpc-protobuf" % grpcJavaVersion,
      "io.grpc" % "grpc-stub" % grpcJavaVersion
    ),
    // flatPackage=true emits all messages flat into the java_package
    // (org.apache.spark.connect.proto) so `proto.DataType`, `proto.Relation`,
    // `proto.Plan` resolve exactly like the upstream Spark Connect client -
    // instead of per-file subpackages (proto.types.DataType, ...).
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true, flatPackage = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

// core: the public Spark Connect client API (SparkSession, DataFrame, Column,
// functions, types) plus the gRPC transport and Arrow result decoding.
lazy val core = (project in file("core"))
  .dependsOn(proto)
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3",
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-vector" % arrowVersion,
      "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
      "io.grpc" % "grpc-netty" % grpcJavaVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.scalameta" %% "munit" % munitVersion % Test
    ),
    testFrameworks += new TestFramework("munit.Framework")
  )

// examples: runnable usage examples, not published.
lazy val examples = (project in file("examples"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3-examples",
    publish / skip := true,
    libraryDependencies += "org.slf4j" % "slf4j-simple" % slf4jVersion
  )

// NOTE: docs are a mkdocs site owned by the docs-ci lane (no sbt module).

lazy val root = (project in file("."))
  .aggregate(proto, core, examples)
  .settings(
    name := "spark-connect-scala3-root",
    publish / skip := true
  )
