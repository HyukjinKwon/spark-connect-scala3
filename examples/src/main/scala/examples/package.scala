package examples

import org.apache.spark.sql.SparkSession

/**
 * Shared helpers for the example programs.
 *
 * Every example connects to the Spark Connect server named by the `SPARK_REMOTE` environment
 * variable, defaulting to a local server on the standard port.
 */
private[examples] object SparkConnect:

  /** The connection string to use, from `$SPARK_REMOTE` or the local default. */
  def remote: String = sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")

  /** Build a session, run `body`, then always stop the session. */
  def withSession(body: SparkSession => Unit): Unit =
    val spark = SparkSession.builder().remote(remote).getOrCreate()
    try body(spark)
    finally spark.stop()
