package examples

import org.apache.spark.sql.functions.*

/**
 * The smallest end-to-end program: connect, build a range, project a derived column, and print the
 * result.
 *
 * {{{
 * sbt "examples/runMain examples.QuickStart"
 * }}}
 */
object QuickStart:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    val df = spark.range(1, 6).select($"id", ($"id" * $"id").as("square"))

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
  }
