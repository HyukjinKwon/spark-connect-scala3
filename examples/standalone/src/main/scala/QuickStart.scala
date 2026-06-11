import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

/**
 * A minimal, self-contained Spark Connect application.
 *
 * Start a Spark Connect server, then run:
 * {{{
 *   SPARK_REMOTE=sc://localhost:15002 sbt run
 * }}}
 */
@main def quickStart(): Unit =
  val remote = sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")

  val spark = SparkSession.builder
    .remote(remote)
    .appName("spark-connect-scala3-standalone")
    .getOrCreate()

  try
    println(s"Connected to Apache Spark ${spark.version} at $remote")

    // Build a lazy plan and run it on the server; results stream back as Arrow.
    val byBucket = spark
      .range(1, 1000)
      .select(col("id"), (col("id") % 3).as("bucket"))
      .groupBy("bucket")
      .agg(count("*").as("n"), sum("id").as("total"))
      .orderBy("bucket")

    byBucket.show()

    val total = byBucket.agg(sum("total").as("grand_total")).collect().head.getLong(0)
    println(s"grand total = $total")
  finally spark.stop()
