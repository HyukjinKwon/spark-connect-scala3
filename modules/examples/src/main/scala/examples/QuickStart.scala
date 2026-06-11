package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * The smallest end-to-end program: connect, build a range, project a derived column, and run a
 * couple of actions.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.QuickStart"
 * }}}
 *
 * Pass a connection string as the first argument to target a different server; otherwise the
 * `SPARK_REMOTE` environment variable is used, defaulting to a local server.
 */
object QuickStart {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("quickstart").getOrCreate()
    try {
      val df = spark.range(1, 6).select(col("id"), (col("id") * col("id")).as("square"))

      df.show()
      // +---+------+
      // | id|square|
      // +---+------+
      // |  1|     1|
      // |  2|     4|
      // |  3|     9|
      // |  4|    16|
      // |  5|    25|
      // +---+------+

      println(s"row count = ${df.count()}")
    } finally {
      spark.stop()
    }
  }
}
