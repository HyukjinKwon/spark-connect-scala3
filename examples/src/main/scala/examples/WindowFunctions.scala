package examples

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*

/**
 * Window functions: per-department salary ranking and running totals.
 *
 * {{{
 * sbt "examples/runMain examples.WindowFunctions"
 * }}}
 */
object WindowFunctions:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    val employees = Seq(
      ("eng", "Ada", 120),
      ("eng", "Alan", 110),
      ("eng", "Grace", 130),
      ("sales", "Tom", 90),
      ("sales", "Sue", 95),
      ("sales", "Ravi", 95)
    ).toDF("dept", "name", "salary")

    val byDeptSalary = Window.partitionBy($"dept").orderBy($"salary".desc)

    employees
      .select(
        $"dept",
        $"name",
        $"salary",
        rank().over(byDeptSalary).as("rank"),
        dense_rank().over(byDeptSalary).as("dense_rank"),
        sum($"salary").over(byDeptSalary).as("running_total")
      )
      .orderBy($"dept", $"rank")
      .show()
  }
