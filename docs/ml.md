# Machine learning

`spark-connect-scala3` exposes Spark ML over Spark Connect under
`org.apache.spark.ml.*`, mirroring Apache Spark's own Scala ML API. Estimators,
transformers, models, evaluators, pipelines, and tuning all keep Spark's class
names, params, defaults, and method names, so Spark ML code reads the same. Fitting,
transforming, evaluating, and model attributes run on the server, which caches
fitted models under an id; the client holds a reference to that id.

The server must be Spark Connect 4.0 or newer. Summary recovery after a model is
offloaded, and cache cleanup when a session closes, need 4.1 or newer; on 4.0 they
are skipped, and the server releases the models when the session ends.

## Quick start

```scala
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = spark.sql(
  "SELECT CAST(IF(id < 30, 0, 1) AS DOUBLE) AS label, CAST(id AS DOUBLE) AS x, " +
    "CAST(id * id % 7 AS DOUBLE) AS y FROM range(60)")

val features = new VectorAssembler().setInputCols(Array("x", "y")).setOutputCol("features")
val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01)
val model = lr.fit(features.transform(df))

println(model.coefficients)            // a Vector, read from the server
println(model.summary.accuracy)        // the training summary
model.transform(features.transform(df)).show()
model.release()                        // evict the cached model now (optional)
```

Vectors and matrices are `org.apache.spark.ml.linalg` types. A vector column comes
back in a `Row` as a struct; decode it with `VectorUDT.deserialize(row.getStruct(i))`
(and `MatrixUDT.deserialize` for matrices). `show()` prints vector columns in their
struct form.

## Pipelines and tuning

`Pipeline` and `PipelineModel` chain stages as in Spark. `CrossValidator` and
`TrainValidationSplit` run the folds from the client, the way PySpark does over
Connect: each parameter map is fit and scored on the server, and the best map is
refit on the full dataset. Build parameter grids with `ParamGridBuilder`.

```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

val pipeline = new Pipeline().setStages(Array(assembler, lr))
val grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.1)).build()
val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEstimatorParamMaps(grid)
  .setEvaluator(new BinaryClassificationEvaluator())
  .setNumFolds(3)
val cvModel = cv.fit(df)
cvModel.avgMetrics
```

## Persistence

Every estimator, transformer, evaluator, model, pipeline, and tuning model has
`save(path)`, `write.overwrite().save(path)`, and a companion `load(path)`. Saved
paths are on the server's filesystem.

```scala
model.write.overwrite().save("/tmp/lr-model")
val loaded = LogisticRegressionModel.load("/tmp/lr-model")
```

## Statistics and functions

- `org.apache.spark.ml.stat.Correlation`, `ChiSquareTest`, and
  `KolmogorovSmirnovTest` return DataFrames.
- `org.apache.spark.ml.stat.Summarizer` builds per-element vector statistics, for
  example `df.select(Summarizer.metrics("mean", "variance").summary(col("features")))`.
- `org.apache.spark.ml.functions.vector_to_array` and `array_to_vector` convert
  between vector and array columns.

## Implemented operators

| Family | Operators |
| ------ | --------- |
| Classification | `LogisticRegression` (with binary and multiclass summaries), `LinearSVC`, `NaiveBayes` |
| Regression | `LinearRegression` (with training summary) |
| Clustering | `KMeans` (with training summary), `BisectingKMeans`, `GaussianMixture` |
| Recommendation | `ALS` (factors and recommendations) |
| Frequent patterns | `FPGrowth` |
| Scalers and encoders | `StandardScaler`, `MinMaxScaler`, `MaxAbsScaler`, `StringIndexer`, `OneHotEncoder`, `Imputer` |
| Text and counts | `Tokenizer`, `RegexTokenizer`, `StopWordsRemover`, `NGram`, `HashingTF`, `CountVectorizer`, `IDF` |
| Vector and feature transformers | `VectorAssembler`, `VectorSlicer`, `VectorSizeHint`, `Interaction`, `FeatureHasher`, `Binarizer`, `Bucketizer`, `Normalizer`, `PolynomialExpansion`, `DCT`, `ElementwiseProduct`, `IndexToString`, `SQLTransformer` |
| Dimensionality reduction | `PCA` |
| Evaluators | `RegressionEvaluator`, `BinaryClassificationEvaluator`, `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, `MultilabelClassificationEvaluator` |
| Tuning | `ParamGridBuilder`, `CrossValidator`, `CrossValidatorModel`, `TrainValidationSplit`, `TrainValidationSplitModel` |
| Pipelines | `Pipeline`, `PipelineModel` |
| Statistics | `Correlation`, `ChiSquareTest`, `KolmogorovSmirnovTest`, `Summarizer` |

Model attributes and methods are those the server allowlists for each model
(`MLUtils.invokeMethodAllowed`), for example `coefficients`, `intercept`,
`clusterCenters`, `predict`, `recommendForAllUsers`, `summary`, and `evaluate`
where it is implemented.

## Not implemented yet

The server allowlists more operators than this client exposes. Not yet
implemented:

- Estimators: decision trees, random forests, gradient-boosted trees, factorization
  machines, multilayer perceptron, AFT survival regression, isotonic regression,
  generalized linear regression, LDA, RFormula, RobustScaler, ChiSqSelector,
  UnivariateFeatureSelector, VarianceThresholdSelector, VectorIndexer, Word2Vec,
  TargetEncoder, and the LSH estimators (`BucketedRandomProjectionLSH`, `MinHashLSH`).
  Their models are not exposed either.
- Model methods for the implemented models that the server allowlists but this
  client does not wrap, such as `evaluate` on `LinearSVC` and `NaiveBayes`, and
  the `GetCacheInfo` and `GetModelSize` commands.

## Differences from Spark

- Models are held on the server. `model.release()` evicts a model; the client does
  not release models when they are garbage collected.
- Tuning runs its fits one after another, so `parallelism` is accepted but has no
  effect. Folds use the same seeded `rand` split as PySpark.
- `Pipeline`, `CrossValidator`, and `TrainValidationSplit` store their metadata as a
  line-based text file, not PySpark's JSON layout, and their stage directories follow
  Spark's `stages/<index>_<uid>` pattern. Tuning persists the parameter maps of the
  top-level estimator only.
- `StopWordsRemover` has no client-side default for `stopWords`; set it explicitly.
- `Summarizer` aliases its results by metric name (`mean`, `sum`, ...), not by the
  column expression.
