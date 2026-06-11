package examples

import org.apache.spark.sql.{Observation, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Exercises the integrated feature surface against a live server to flush out bugs. */
object FeatureCheck {
  def main(args: Array[String]): Unit = {
    val remote = if (args.nonEmpty) args(0) else "sc://localhost:15099"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    var failures = 0
    def check(name: String)(body: => Unit): Unit =
      try { body; println(s"[PASS] $name") }
      catch { case e: Throwable => failures += 1; println(s"[FAIL] $name: ${e.getClass.getSimpleName}: ${e.getMessage}") }

    check("createDataFrame + collect roundtrip") {
      val schema = StructType(Array(
        StructField("id", IntegerType), StructField("name", StringType),
        StructField("score", DoubleType), StructField("active", BooleanType)))
      val rows = Seq(Row(1, "alice", 9.5, true), Row(2, "bob", 7.0, false), Row(3, null, 3.0, true))
      val df = spark.createDataFrame(rows, schema)
      val got = df.orderBy("id").collect()
      assert(got.length == 3, s"len ${got.length}")
      assert(got(0).getInt(0) == 1 && got(0).getString(1) == "alice" && got(0).getDouble(2) == 9.5)
      assert(got(2).isNullAt(1), "expected null name")
    }

    check("na.fill / na.drop") {
      val schema = StructType(Array(StructField("a", StringType), StructField("b", IntegerType)))
      val df = spark.createDataFrame(Seq(Row("x", 1), Row(null, 2), Row("y", null)), schema)
      assert(df.na.drop().collect().length == 1)
      assert(df.na.fill("NA").filter(col("a") === "NA").collect().length == 1)
    }

    check("stat.describe / summary / corr") {
      val df = spark.range(1, 100).select(col("id"), (col("id") * 2).as("d"))
      assert(df.describe("id").collect().nonEmpty)
      assert(df.summary().collect().nonEmpty)
      val c = df.stat.corr("id", "d")
      assert(math.abs(c - 1.0) < 1e-6, s"corr=$c")
    }

    check("catalog: temp view + listTables + tableExists") {
      spark.range(5).createOrReplaceTempView("cat_demo")
      assert(spark.catalog.tableExists("cat_demo"))
      assert(spark.catalog.listTables().collect().exists(_.getString(0) == "cat_demo"))
      assert(spark.table("cat_demo").count() == 5)
      assert(spark.catalog.currentDatabase.nonEmpty)
    }

    check("observe metrics") {
      val obs = new Observation("metrics")
      val df = spark.range(10).observe(obs, count(lit(1)).as("cnt"), sum("id").as("total"))
      df.collect()
      val m = obs.get
      assert(m("cnt") == 10L, s"cnt=${m.get("cnt")}")
      assert(m("total") == 45L, s"total=${m.get("total")}")
    }

    check("write.parquet + read.parquet roundtrip") {
      val dir = java.nio.file.Files.createTempDirectory("scs3").toString + "/data"
      spark.range(20).select(col("id"), (col("id") % 4).as("g")).write.mode("overwrite").parquet(dir)
      val back = spark.read.parquet(dir)
      assert(back.count() == 20, s"count=${back.count()}")
    }

    check("window: row_number") {
      val schema = StructType(Array(StructField("g", StringType), StructField("v", IntegerType)))
      val df = spark.createDataFrame(
        Seq(Row("a", 3), Row("a", 1), Row("b", 2), Row("a", 2)), schema)
      val w = Window.partitionBy("g").orderBy(col("v"))
      val ranked = df.select(col("g"), col("v"), row_number().over(w).as("rn"))
      val rows = ranked.filter(col("g") === "a" && col("rn") === 1).collect()
      assert(rows.length == 1 && rows(0).getInt(1) == 1, "min v for a should rank 1")
    }

    check("functions: string + conditional") {
      val df = spark.createDataFrame(
        Seq(Row("Hello"), Row("World")), StructType(Array(StructField("s", StringType))))
      val r = df.select(upper(col("s")).as("u"), length(col("s")).as("l")).orderBy("u").collect()
      assert(r(0).getString(0) == "HELLO" && r(0).getInt(1) == 5)
    }

    check("join with condition") {
      val a = spark.range(5).select(col("id").as("k"), lit("a").as("av"))
      val b = spark.range(3).select(col("id").as("k"), lit("b").as("bv"))
      val joined = a.join(b, a("k") === b("k"))
      assert(joined.count() == 3, s"count=${joined.count()}")
    }

    spark.stop()
    println(s"\n==== ${if (failures == 0) "ALL FEATURE CHECKS PASSED" else s"$failures FAILURE(S)"} ====")
    if (failures > 0) sys.exit(1)
  }
}
