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
