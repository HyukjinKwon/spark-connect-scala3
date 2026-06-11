package examples

import org.apache.spark.sql.functions.*

/**
 * Round-trips a DataFrame through Parquet on the *server's* filesystem.
 *
 * The output path is resolved by the Spark Connect server, not your local machine. Override it with
 * the first program argument.
 *
 * {{{
 * sbt "examples/runMain examples.ReadWrite /tmp/sc3-example"
 * }}}
 */
object ReadWrite:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    val path = args.headOption.getOrElse("/tmp/sc3-example-people")

    val people = Seq(
      (1, "Ada", 36),
      (2, "Alan", 41),
      (3, "Grace", 45)
    ).toDF("id", "name", "age")

    // Write.
    people.write.mode("overwrite").parquet(path)
    println(s"wrote ${people.count()} rows to $path (server-side)")

    // Read back and transform.
    val reloaded = spark.read.parquet(path)
    reloaded
      .filter($"age" >= 40)
      .select($"name", $"age")
      .orderBy($"age".desc)
      .show()
  }
