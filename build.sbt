/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Spark Connect client for Scala 3: multi-module build.

ThisBuild / organization := "io.github.hyukjinkwon"
ThisBuild / organizationName := "Hyukjin Kwon"
ThisBuild / scalaVersion := "3.3.6"
// Version is derived from git tags by sbt-dynver (via sbt-ci-release): a tag like
// `v0.1.0` publishes `0.1.0`; untagged builds get a `-SNAPSHOT` version.
ThisBuild / versionScheme := Some("early-semver")
// Publish through the Sonatype Central Portal (central.sonatype.com). The legacy
// OSSRH hosts (oss.sonatype.org / s01.oss.sonatype.org) were sunset in 2025, so new
// namespaces publish via the Central Portal, which sbt-ci-release targets when the
// credential host is set to central.sonatype.com.
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"
ThisBuild / homepage := Some(url("https://github.com/HyukjinKwon/spark-connect-scala3"))
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "HyukjinKwon",
    "Hyukjin Kwon",
    "gurwls223@apache.org",
    url("https://github.com/HyukjinKwon")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/HyukjinKwon/spark-connect-scala3"),
    "scm:git:https://github.com/HyukjinKwon/spark-connect-scala3.git"
  )
)

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-language:implicitConversions"
)

val grpcVersion = "1.65.1"
val arrowVersion = "17.0.0"
val sparkConnectProtoVersion = "4.1.2" // Spark version the protobufs are sourced from.

// JVM module-access flags required by Arrow's off-heap memory on JDK 17+.
val arrowJvmOptions = Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

// Common settings shared by published modules.
lazy val commonSettings = Seq(
  Test / fork := true,
  Test / testForkedParallel := false,
  Test / javaOptions ++= arrowJvmOptions,
  run / fork := true,
  run / javaOptions ++= arrowJvmOptions
)

lazy val root = (project in file("."))
  .aggregate(proto, client, examples)
  .settings(name := "spark-connect-scala3", publish / skip := true)

// Generated gRPC + message classes from the Spark Connect protobuf definitions.
lazy val proto = (project in file("modules/proto"))
  .settings(commonSettings)
  .settings(
    name := "spark-connect-scala3-proto",
    Compile / PB.targets := Seq(
      scalapb.gen(flatPackage = true, grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc" % "grpc-netty-shaded" % grpcVersion,
      "io.grpc" % "grpc-protobuf" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion
    )
  )

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
      // Pure-Java sketch impls (CountMinSketch / BloomFilter) used by stat functions to
      // deserialize the binary produced by the server-side aggregates. Declared as a normal
      // (POM-visible) dependency so downstream users of stat.bloomFilter / countMinSketch get it
      // on their classpath; its only transitive compile dependency is the tiny annotations-only
      // spark-tags jar (all its other dependencies are test-scoped and do not propagate).
      "org.apache.spark" % "spark-sketch_2.13" % "4.1.2",
      "org.scalameta" %% "munit" % "1.0.2" % Test
    ),
    testFrameworks += new TestFramework("munit.Framework"),
    // Powers `bin/spark-connect-shell`: `sbt client/console` opens a REPL with a `spark`
    // session already connected to the address in the `spark.connect.shell.remote` property.
    Compile / console / initialCommands := {
      val remote = sys.props.getOrElse("spark.connect.shell.remote", "sc://localhost:15002")
      s"""import org.apache.spark.sql.SparkSession
         |import org.apache.spark.sql.functions._
         |val spark = SparkSession.builder.remote("$remote").getOrCreate()
         |import spark.implicits._
         |println("Connected to Spark Connect " + spark.version + " at $remote; the `spark` session is ready.")
         |""".stripMargin
    },
    Compile / doc / scalacOptions ++= Seq(
      "-project",
      "Spark Connect for Scala 3",
      "-siteroot",
      "docs"
    )
  )

// Runnable examples (not published).
lazy val examples = (project in file("modules/examples"))
  .dependsOn(client)
  .settings(commonSettings)
  .settings(name := "spark-connect-scala3-examples", publish / skip := true, run / fork := true)
