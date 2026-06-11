// Spark Connect client for Scala 3 — multi-module build.

ThisBuild / organization := "io.github.hyukjinkwon"
ThisBuild / organizationName := "Hyukjin Kwon"
ThisBuild / scalaVersion := "3.3.6"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / homepage := Some(url("https://github.com/HyukjinKwon/spark-connect-scala3"))
ThisBuild / licenses := Seq(
  "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "HyukjinKwon",
    "Hyukjin Kwon",
    "gurwls223@apache.org",
    url("https://github.com/HyukjinKwon")))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/HyukjinKwon/spark-connect-scala3"),
    "scm:git:https://github.com/HyukjinKwon/spark-connect-scala3.git"))

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-language:implicitConversions")

val grpcVersion = "1.65.1"
val arrowVersion = "17.0.0"
val sparkConnectProtoVersion = "4.1.2" // Spark version the protobufs are sourced from.

// JVM module-access flags required by Arrow's off-heap memory on JDK 17+.
val arrowJvmOptions = Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true")

// Common settings shared by published modules.
lazy val commonSettings = Seq(
  Test / fork := true,
  Test / testForkedParallel := false,
  Test / javaOptions ++= arrowJvmOptions,
  run / fork := true,
  run / javaOptions ++= arrowJvmOptions)

lazy val root = (project in file("."))
  .aggregate(proto, client, examples)
  .settings(
    name := "spark-connect-scala3",
    publish / skip := true)

// Generated gRPC + message classes from the Spark Connect protobuf definitions.
lazy val proto = (project in file("modules/proto"))
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3-proto",
    Compile / PB.targets := Seq(
      scalapb.gen(flatPackage = true, grpc = true) -> (Compile / sourceManaged).value / "scalapb"),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc" % "grpc-netty-shaded" % grpcVersion,
      "io.grpc" % "grpc-protobuf" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion))

// The public Spark Connect client API (org.apache.spark.sql.*).
lazy val client = (project in file("modules/client"))
  .dependsOn(proto)
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3-client",
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-vector" % arrowVersion,
      "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0",
      "org.slf4j" % "slf4j-api" % "2.0.13",
      "org.scalameta" %% "munit" % "1.0.2" % Test),
    testFrameworks += new TestFramework("munit.Framework"),
    Compile / doc / scalacOptions ++= Seq(
      "-project",
      "Spark Connect for Scala 3",
      "-siteroot",
      "docs"))

// Runnable examples (not published).
lazy val examples = (project in file("modules/examples"))
  .dependsOn(client)
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3-examples",
    publish / skip := true,
    run / fork := true)
