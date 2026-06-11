package examples

import org.apache.spark.sql.functions.*

/**
 * Classic word count: split lines into words, explode to one row per word, then group and count.
 *
 * {{{
 * sbt "examples/runMain examples.WordCount"
 * }}}
 */
object WordCount:
  def main(args: Array[String]): Unit = SparkConnect.withSession { spark =>
    import spark.implicits.*

    val lines = Seq(
      "the quick brown fox",
      "the lazy dog",
      "the quick dog"
    ).toDF("line")

    val counts = lines
      .select(explode(split($"line", " ")).as("word"))
      .groupBy($"word")
      .count()
      .orderBy($"count".desc, $"word")

    counts.show()
    // +-----+-----+
    // | word|count|
    // +-----+-----+
    // |  the|    3|
    // |  dog|    2|
    // |quick|    2|
    // |brown|    1|
    // |  fox|    1|
    // | lazy|    1|
    // +-----+-----+
  }
