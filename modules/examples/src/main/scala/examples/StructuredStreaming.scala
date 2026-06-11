package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * Structured Streaming: read the built-in `rate` source, transform it, and write to the in-memory
 * sink, then query the sink as a table.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.StructuredStreaming"
 * }}}
 */
object StructuredStreaming {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("structured-streaming").getOrCreate()
    try {
      val rates = spark.readStream
        .format("rate")
        .option("rowsPerSecond", 10)
        .load()

      val query = rates
        .selectExpr("value", "value % 5 AS bucket")
        .writeStream
        .format("memory")
        .queryName("rates")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      // Wait for a batch of data, then inspect the in-memory sink as a table.
      query.processAllAvailable()
      spark
        .sql("SELECT bucket, count(*) AS n FROM rates GROUP BY bucket ORDER BY bucket")
        .show()

      query.stop()
    } finally spark.stop()
  }
}
