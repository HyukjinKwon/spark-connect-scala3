package examples

import org.apache.spark.sql.functions.*

/**
 * Mixing the DataFrame API with raw SQL via temporary views.
 *
 * {{{
 * sbt "examples/runMain examples.SqlAndViews"
 * }}}
 */
object SqlAndViews:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    // Plain SQL returns a DataFrame.
    spark.sql("SELECT id, id * 2 AS doubled FROM range(5)").show()

    // Register a DataFrame as a temp view, then query it with SQL.
    val nums = spark.range(0, 20).withColumn("bucket", $"id" % 4)
    nums.createOrReplaceTempView("nums")

    spark
      .sql("""
        SELECT bucket, count(*) AS n, avg(id) AS avg_id
        FROM nums
        GROUP BY bucket
        ORDER BY bucket
      """)
      .show()

    // Parameterised SQL keeps user input out of the query string.
    spark
      .sql("SELECT * FROM nums WHERE bucket = :b", Map("b" -> 2))
      .show()
  }
