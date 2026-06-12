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
// A self-contained downstream application that uses spark-connect-scala3-client as
// an ordinary library dependency. This project is intentionally NOT part of the
// client's own sbt build - it shows exactly what a user's own project looks like.

ThisBuild / scalaVersion := "3.3.6"

lazy val app = (project in file("."))
  .settings(
    name := "spark-connect-scala3-standalone-example",
    libraryDependencies += "io.github.hyukjinkwon" %% "spark-connect-scala3-client" % "0.1.0",
    // Apache Arrow accesses internal NIO buffers; open the modules on the application
    // JVM (required on JDK 17 and newer), and fork so these options take effect.
    run / fork := true,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
