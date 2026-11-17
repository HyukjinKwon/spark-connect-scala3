package org.apache.spark.ml

import scala.io.Source

import org.apache.spark.ml.evaluation.Evaluator

/**
 * Checks that every class the server allowlists (the Spark 4.3.0 `META-INF/services` files,
 * vendored under `resources/ml/allowlist`) has a client class of the same name and type. This is
 * how the coverage of the ML API is proven: a missing operator fails here rather than silently.
 */
class AllowlistCoverageSuite extends munit.FunSuite {

  private def names(file: String): Seq[String] = {
    val in = getClass.getResourceAsStream(s"/ml/allowlist/$file")
    assert(in != null, s"missing resource $file")
    try
      Source
        .fromInputStream(in)
        .getLines()
        .map(_.trim)
        .filter(l => l.nonEmpty && !l.startsWith("#"))
        .toList
    finally in.close()
  }

  private val estimators = names("org.apache.spark.ml.Estimator")
  private val transformers = names("org.apache.spark.ml.Transformer")
  private val evaluators = names("org.apache.spark.ml.evaluation.Evaluator")

  test("the allowlists list 42 estimators, 61 transformers and 6 evaluators") {
    assertEquals(estimators.length, 42)
    assertEquals(transformers.length, 61)
    assertEquals(evaluators.length, 6)
  }

  test("every allowlisted estimator has a client Estimator class") {
    val missing = estimators.filterNot { name =>
      try classOf[Estimator[_]].isAssignableFrom(Class.forName(name))
      catch { case _: ClassNotFoundException => false }
    }
    assertEquals(missing, Nil, s"allowlisted estimators without a client class")
  }

  test("every allowlisted transformer (models included) has a client Transformer class") {
    val missing = transformers.filterNot { name =>
      try classOf[Transformer].isAssignableFrom(Class.forName(name))
      catch { case _: ClassNotFoundException => false }
    }
    assertEquals(missing, Nil, s"allowlisted transformers without a client class")
  }

  test("every allowlisted evaluator has a client Evaluator class") {
    val missing = evaluators.filterNot { name =>
      try classOf[Evaluator].isAssignableFrom(Class.forName(name))
      catch { case _: ClassNotFoundException => false }
    }
    assertEquals(missing, Nil, s"allowlisted evaluators without a client class")
  }
}
