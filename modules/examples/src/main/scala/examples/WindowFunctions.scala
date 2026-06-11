package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Window functions: per-department salary ranking and running totals.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.WindowFunctions"
 * }}}
 */
object WindowFunctions {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("window-functions").getOrCreate()
    try {
      val employees = spark.sql(
        "SELECT * FROM VALUES " +
          "('eng', 'Ada', 120), ('eng', 'Alan', 110), ('eng', 'Grace', 130), " +
          "('sales', 'Tom', 90), ('sales', 'Sue', 95), ('sales', 'Ravi', 95) " +
          "AS t(dept, name, salary)"
      )

      val byDeptSalary = Window.partitionBy(col("dept")).orderBy(col("salary").desc)

      employees
        .select(
          col("dept"),
          col("name"),
          col("salary"),
          rank().over(byDeptSalary).as("rank"),
          dense_rank().over(byDeptSalary).as("dense_rank"),
          sum(col("salary")).over(byDeptSalary).as("running_total")
        )
        .orderBy(col("dept"), col("rank"))
        .show()
    } finally spark.stop()
  }
}
