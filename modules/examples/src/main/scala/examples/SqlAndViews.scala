package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Mixing the DataFrame API with raw SQL through temporary views, including parameterised SQL.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.SqlAndViews"
 * }}}
 */
object SqlAndViews {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("sql-and-views").getOrCreate()
    try {
      // Plain SQL returns a DataFrame.
      spark.sql("SELECT id, id * 2 AS doubled FROM range(5)").show()

      // Register a DataFrame as a temp view, then query it with SQL.
      val nums = spark.range(0, 20).withColumn("bucket", col("id") % 4)
      nums.createOrReplaceTempView("nums")
      spark.sql("SELECT bucket, count(*) AS n FROM nums GROUP BY bucket ORDER BY bucket").show()

      // Parameterised SQL keeps user input out of the query string.
      spark.sql("SELECT * FROM nums WHERE bucket = :b", Map("b" -> 2)).show()
    } finally spark.stop()
  }
}
