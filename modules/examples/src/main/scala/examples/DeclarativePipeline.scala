package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Spark Declarative Pipelines (SDP): build a small dataflow graph of a source materialized view and
 * a derived one, then run it. Requires an Apache Spark 4.1 or newer server.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.DeclarativePipeline"
 * }}}
 */
object DeclarativePipeline {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    // The server requires an absolute URI for pipeline storage (checkpoint/metadata).
    val storage = if (args.length > 1) args(1) else "file:///tmp/sc3-pipeline-storage"
    val spark = SparkSession.builder.remote(remote).appName("declarative-pipeline").getOrCreate()
    try {
      val pipe = spark.pipeline()
      pipe.createMaterializedView("numbers", Some(spark.range(0, 100)))
      pipe.createMaterializedView("evens", Some(pipe.read("numbers").where(col("id") % 2 === 0)))

      val events = pipe.startRun(storage = Some(storage))
      events.foreach(e => println(s"[pipeline] ${e.message.getOrElse("")}"))

      spark.read.table("evens").orderBy("id").show(5)
    } finally spark.stop()
  }
}
