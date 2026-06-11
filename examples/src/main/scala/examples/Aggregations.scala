package examples

import org.apache.spark.sql.functions.*

/**
 * Grouped aggregation with several aggregate functions over synthetic sales data.
 *
 * {{{
 * sbt "examples/runMain examples.Aggregations"
 * }}}
 */
object Aggregations:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    val sales = Seq(
      ("KR", "book", 12.0),
      ("KR", "pen", 3.5),
      ("KR", "book", 9.0),
      ("US", "book", 15.0),
      ("US", "pen", 2.0),
      ("US", "pen", 2.5)
    ).toDF("country", "item", "amount")

    val summary = sales
      .groupBy($"country")
      .agg(
        count("*").as("orders"),
        round(sum($"amount"), 2).as("total"),
        round(avg($"amount"), 2).as("avg"),
        max($"amount").as("max")
      )
      .orderBy($"country")

    summary.show()

    // Conditional aggregation: total spent on books per country.
    sales
      .groupBy($"country")
      .agg(
        round(sum(when($"item" === "book", $"amount").otherwise(0.0)), 2)
          .as("book_total")
      )
      .orderBy($"country")
      .show()
  }
