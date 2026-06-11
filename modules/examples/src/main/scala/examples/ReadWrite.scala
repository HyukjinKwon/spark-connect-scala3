package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Round-trips a DataFrame through Parquet on the server's filesystem.
 *
 * The path is resolved by the Spark Connect server, not your local machine. Pass the connection
 * string as the first argument and an output path as the second.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.ReadWrite"
 * }}}
 */
object ReadWrite {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val path = if (args.length > 1) args(1) else "/tmp/sc3-example-people"
    val spark = SparkSession.builder.remote(remote).appName("read-write").getOrCreate()
    try {
      val people = spark.sql(
        "SELECT * FROM VALUES (1, 'Ada', 36), (2, 'Alan', 41), (3, 'Grace', 45) AS t(id, name, age)"
      )

      people.write.mode("overwrite").parquet(path)
      println(s"wrote ${people.count()} rows to $path (server-side)")

      spark.read
        .parquet(path)
        .filter(col("age") >= 40)
        .select(col("name"), col("age"))
        .orderBy(col("age").desc)
        .show()
    } finally spark.stop()
  }
}
