package examples

/**
 * Spark Declarative Pipelines (SDP): build a small dataflow graph of a source view and a derived
 * table, then run it.
 *
 * {{{
 * sbt "examples/runMain examples.DeclarativePipeline"
 * }}}
 */
object DeclarativePipeline:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    val pipe = spark.pipeline()

    // A source materialized view, and a derived table that reads from it.
    pipe.createMaterializedView("numbers", spark.range(0, 100))
    pipe.createMaterializedView("evens", pipe.read("numbers").where($"id" % 2 === 0))

    // Resolve the graph and run an update, printing the events the server emits.
    // The server requires a storage location for pipeline checkpoint/metadata.
    val storage = args.headOption.getOrElse("file:///tmp/sc3-pipeline-storage")
    val events = pipe.startRun(storage = storage)
    events.foreach(e => println(s"[pipeline] ${e.message}"))

    spark.read.table("evens").orderBy($"id").show(5)
  }
