package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Grouped aggregation with several aggregate functions over synthetic sales data.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.Aggregations"
 * }}}
 */
object Aggregations {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("aggregations").getOrCreate()
    try {
      val sales = spark.sql(
        "SELECT * FROM VALUES " +
          "('KR', 'book', 12.0), ('KR', 'pen', 3.5), ('KR', 'book', 9.0), " +
          "('US', 'book', 15.0), ('US', 'pen', 2.0), ('US', 'pen', 2.5) " +
          "AS t(country, item, amount)")

      // One row per country with a handful of aggregates.
      sales
        .groupBy("country")
        .agg(
          count("*").as("orders"),
          round(sum("amount"), 2).as("total"),
          round(avg("amount"), 2).as("avg"),
          max("amount").as("max"))
        .orderBy("country")
        .show()

      // Conditional aggregation: total spent on books per country.
      sales
        .groupBy("country")
        .agg(
          round(sum(when(col("item") === "book", col("amount")).otherwise(0.0)), 2)
            .as("book_total"))
        .orderBy("country")
        .show()
    } finally {
      spark.stop()
    }
  }
}
