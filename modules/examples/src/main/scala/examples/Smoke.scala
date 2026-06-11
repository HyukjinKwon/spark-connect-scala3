package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Smoke test run against a live Spark Connect server. */
object Smoke {
  def main(args: Array[String]): Unit = {
    val remote = if (args.nonEmpty) args(0) else "sc://localhost:15099"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    try {
      println("== version ==")
      println(spark.version)

      println("== range/filter/collect ==")
      val rows = spark.range(10).filter(col("id") % 2 === 0).collect()
      println(rows.map(_.getLong(0)).mkString(", "))

      println("== show ==")
      spark.range(5).select(col("id"), (col("id") * 2).as("doubled")).show()

      println("== sql + groupBy/agg ==")
      spark
        .range(1, 10)
        .select(col("id"), (col("id") % 3).as("bucket"))
        .groupBy("bucket")
        .agg(count(lit(1)).as("n"), sum("id").as("total"))
        .orderBy("bucket")
        .show()

      println("== count ==")
      println(spark.range(100).count())

      println("== sql ==")
      spark.sql("select 1 as a, 'hello' as b").show()
    } finally spark.stop()
  }
}
