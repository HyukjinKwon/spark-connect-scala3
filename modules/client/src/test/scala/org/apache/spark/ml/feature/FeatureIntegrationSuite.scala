/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.ml.feature

import org.apache.spark.ml.{MlTestData, Transformer}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, col, lit}
import org.apache.spark.ml.MlRemoteSuite

class FeatureTransformersIntegrationSuite extends MlRemoteSuite {

  private def firstVector(df: org.apache.spark.sql.DataFrame, name: String): Vector =
    VectorUDT.deserialize(df.select(name).head().getStruct(0))

  test("VectorAssembler builds a vector column from numeric columns") {
    withSpark { spark =>
      val df =
        spark.sql("SELECT CAST(id AS DOUBLE) AS x, CAST(id * 2 AS DOUBLE) AS y FROM range(3)")
      val out = new VectorAssembler().setInputCols(Array("x", "y")).setOutputCol("v").transform(df)
      assertEquals(firstVector(out, "v").toArray.toSeq, Seq(0.0, 0.0))
      assertEquals(out.count(), 3L)
    }
  }

  test("Tokenizer and RegexTokenizer split text into words") {
    withSpark { spark =>
      val docs = MlTestData.documents(spark)
      val words = new Tokenizer().setInputCol("text").setOutputCol("words").transform(docs)
      assertEquals(
        words.select("words").head().getSeq[String](0).toSeq,
        Seq("spark", "connect", "ml", "word0")
      )
      val regex = new RegexTokenizer()
        .setInputCol("text")
        .setOutputCol("w")
        .setPattern("\\s+")
        .setMinTokenLength(4)
      assertEquals(regex.transform(docs).select("w").head().getSeq[String](0).length, 3)
    }
  }

  test("Binarizer, Normalizer, DCT and ElementwiseProduct on numeric and vector columns") {
    withSpark { spark =>
      val df = spark.sql("SELECT CAST(id AS DOUBLE) AS x FROM range(4)")
      val bin = new Binarizer().setInputCol("x").setOutputCol("b").setThreshold(1.5).transform(df)
      assertEquals(bin.select("b").collect().map(_.getDouble(0)).toSeq, Seq(0.0, 0.0, 1.0, 1.0))

      val vec = MlTestData.binary(spark)
      val normed =
        new Normalizer().setInputCol("features").setOutputCol("n").setP(1.0).transform(vec)
      val v = firstVector(normed, "n")
      assert(math.abs(v.toArray.map(math.abs).sum - 1.0) < 1e-9 || v.toArray.forall(_ == 0.0))
      val dct = new DCT().setInputCol("features").setOutputCol("d").transform(vec)
      assertEquals(firstVector(dct, "d").size, 2)
      val prod = new ElementwiseProduct()
        .setInputCol("features")
        .setOutputCol("p")
        .setScalingVec(Vectors.dense(2.0, 3.0))
        .transform(vec)
      assertEquals(firstVector(prod, "p").size, 2)
    }
  }

  test("NGram, HashingTF, StopWordsRemover and IndexToString work on token arrays") {
    withSpark { spark =>
      val tokens = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
        .transform(MlTestData.documents(spark))
      val grams = new NGram().setInputCol("words").setOutputCol("grams").setN(2).transform(tokens)
      assertEquals(grams.select("grams").head().getSeq[String](0).length, 3)
      val hashed =
        new HashingTF().setInputCol("words").setOutputCol("h").setNumFeatures(16).transform(tokens)
      assertEquals(firstVector(hashed, "h").size, 16)
      val filtered = new StopWordsRemover()
        .setInputCol("words")
        .setOutputCol("f")
        .setStopWords(Array("spark"))
        .transform(tokens)
      assert(!filtered.select("f").head().getSeq[String](0).contains("spark"))
      val indexed = spark.sql("SELECT CAST(id % 2 AS DOUBLE) AS idx FROM range(4)")
      val labels = new IndexToString()
        .setInputCol("idx")
        .setOutputCol("label")
        .setLabels(Array("no", "yes"))
        .transform(indexed)
      assertEquals(labels.select("label").head().getString(0), "no")
    }
  }

  test("PolynomialExpansion, VectorSlicer, Interaction, VectorSizeHint and SQLTransformer") {
    withSpark { spark =>
      val vec = MlTestData.binary(spark)
      val poly = new PolynomialExpansion()
        .setInputCol("features")
        .setOutputCol("p")
        .setDegree(2)
        .transform(vec)
      assertEquals(firstVector(poly, "p").size, 5)
      val sliced = new VectorSlicer()
        .setInputCol("features")
        .setOutputCol("s")
        .setIndices(Array(1))
        .transform(vec)
      assertEquals(firstVector(sliced, "s").size, 1)
      val hinted = new VectorSizeHint()
        .setInputCol("features")
        .setSize(2)
        .setHandleInvalid("skip")
        .transform(vec)
      assertEquals(hinted.count(), 40L)
      val sql = new SQLTransformer()
        .setStatement("SELECT *, label + 1 AS label1 FROM __THIS__")
        .transform(vec)
      assert(sql.columns.contains("label1"))
      val inter = new Interaction()
        .setInputCols(Array("a", "b"))
        .setOutputCol("i")
        .transform(
          spark.sql("SELECT CAST(id AS DOUBLE) AS a, CAST(id + 1 AS DOUBLE) AS b FROM range(2)")
        )
      assertEquals(inter.count(), 2L)
    }
  }

  test("Bucketizer and FeatureHasher") {
    withSpark { spark =>
      val df =
        spark.sql("SELECT CAST(id AS DOUBLE) AS x, CAST(id % 2 AS STRING) AS c FROM range(6)")
      val buckets = new Bucketizer()
        .setInputCol("x")
        .setOutputCol("bx")
        .setSplits(Array(Double.NegativeInfinity, 2.0, Double.PositiveInfinity))
        .transform(df)
      assertEquals(
        buckets.select("bx").collect().map(_.getDouble(0)).toSeq,
        Seq(0.0, 0.0, 1.0, 1.0, 1.0, 1.0)
      )
      val hasher =
        new FeatureHasher().setInputCols(Array("x", "c")).setOutputCol("f").setNumFeatures(32)
      assertEquals(firstVector(hasher.transform(df), "f").size, 32)
    }
  }

  test("transformers are Transformers and copy their parameters") {
    val tok: Transformer = new Tokenizer().setInputCol("a")
    assertEquals(tok.copy(org.apache.spark.ml.param.ParamMap.empty).uid, tok.uid)
    assertEquals(new Tokenizer().getOutputCol.endsWith("__output"), true)
    assertEquals(new Tokenizer().setInputCol("a").getInputCol, "a")
  }
}

class FeatureEstimatorsIntegrationSuite extends MlRemoteSuite {

  test("scalers fit mean, bounds and max-abs attributes") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val std = new StandardScaler().setInputCol("features").setWithMean(true).fit(df)
      assertEquals(std.mean.size, 2)
      assertEquals(std.std.size, 2)
      val mm = new MinMaxScaler().setInputCol("features").fit(df)
      assertEquals(mm.originalMax.size, 2)
      assertEquals(mm.originalMin.size, 2)
      val ma = new MaxAbsScaler().setInputCol("features").fit(df)
      assertEquals(ma.maxAbs.size, 2)
      assertEquals(std.transform(df).count(), 40L)
      std.release(); mm.release(); ma.release()
    }
  }

  test("StringIndexer and OneHotEncoder expose labels and category sizes") {
    withSpark { spark =>
      val df = spark.sql("SELECT CAST(id % 3 AS STRING) AS c FROM range(9)")
      val indexer = new StringIndexer().setInputCol("c").setOutputCol("ci").fit(df)
      assertEquals(indexer.labels.toSeq.sorted, Seq("0", "1", "2"))
      val indexed = indexer.transform(df)
      val encoder =
        new OneHotEncoder().setInputCols(Array("ci")).setOutputCols(Array("oh")).fit(indexed)
      assertEquals(encoder.categorySizes.toSeq, Seq(3))
      indexer.release(); encoder.release()
    }
  }

  test("PCA reports principal components and explained variance") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val pca = new PCA().setInputCol("features").setOutputCol("pc").setK(1).fit(df)
      assertEquals(pca.pc.numRows, 2)
      assertEquals(pca.pc.numCols, 1)
      assertEquals(pca.explainedVariance.size, 1)
      pca.release()
    }
  }

  test("IDF, CountVectorizer and Imputer on token and numeric columns") {
    withSpark { spark =>
      val tokens = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
        .transform(MlTestData.documents(spark))
      val cv = new CountVectorizer().setInputCol("words").setOutputCol("cv").fit(tokens)
      assert(cv.vocabulary.contains("spark"))
      val counts = cv.transform(tokens)
      val idf = new IDF().setInputCol("cv").setOutputCol("tfidf").fit(counts)
      assertEquals(idf.idf.size, cv.vocabulary.length)
      assertEquals(idf.numDocs, 6L)
      val nums = spark.sql("SELECT CAST(IF(id = 1, NULL, id) AS DOUBLE) AS x FROM range(4)")
      val imputer = new Imputer().setInputCols(Array("x")).setOutputCols(Array("x2")).fit(nums)
      assertEquals(imputer.surrogateDF.count(), 1L)
      cv.release(); idf.release(); imputer.release()
    }
  }
}
