package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Join two DataFrames on a shared key, then aggregate the joined result.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.JoinExample"
 * }}}
 */
object JoinExample {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("join-example").getOrCreate()
    try {
      val orders = spark.sql(
        "SELECT * FROM VALUES " +
          "(1, 'KR', 120.0), (2, 'US', 80.0), (3, 'KR', 50.0), (4, 'JP', 30.0) " +
          "AS t(id, country, amount)"
      )

      val regions = spark.sql(
        "SELECT * FROM VALUES ('KR', 'Asia'), ('US', 'Americas'), ('JP', 'Asia') " +
          "AS t(country, region)"
      )

      // Inner join on the shared `country` column, then total by region.
      orders
        .join(regions, "country")
        .groupBy("region")
        .agg(round(sum("amount"), 2).as("total"), count("*").as("orders"))
        .orderBy(col("total").desc)
        .show()

      // A left join keeps every order even when no region matches.
      val partialRegions = spark.sql("SELECT * FROM VALUES ('KR', 'Asia') AS t(country, region)")
      orders
        .join(partialRegions, orders.col("country") === partialRegions.col("country"), "left")
        .select(orders.col("id"), orders.col("country"), col("region"))
        .orderBy("id")
        .show()
    } finally spark.stop()
  }
}
