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

Composite models use Spark's standard layout, the one PySpark 4.3.0 writes over
Connect, so they load in either client:

- `Pipeline` and `PipelineModel`: `metadata` (with `stageUids`) and one directory per
  stage, `stages/<index>_<uid>`, with the index zero-padded to the stage count.
- `CrossValidator`/`CrossValidatorModel` and `TrainValidationSplit`/`TrainValidationSplitModel`:
  `metadata` (with `estimatorParamMaps`, and `avgMetrics`, `stdMetrics` or
  `validationMetrics`), `estimator`, `evaluator`, and `bestModel`. Sub-models are written
  under `subModels/fold<i>/<j>` when `persistSubModels` is set.
- `OneVsRest`/`OneVsRestModel`: `metadata` (with `numClasses` on the model),
  `classifier`, and one `model_<i>` directory per class.

Operators and models are written by the server in Spark's own format. Metadata
is a JSON document with `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`
and `defaultParamMap`. The client writes the same keys for the composite objects.

## Statistics and functions

- `org.apache.spark.ml.stat.Correlation`, `ChiSquareTest`, and
  `KolmogorovSmirnovTest` return DataFrames.
- `org.apache.spark.ml.stat.Summarizer` builds per-element vector statistics, for
  example `df.select(Summarizer.metrics("mean", "variance").summary(col("features")))`.
- `org.apache.spark.ml.functions.vector_to_array` and `array_to_vector` convert
  between vector and array columns.

## Coverage

The client covers every class the server allowlists, in the Spark 4.3.0
`META-INF/services` files: 42 estimators, 61 transformers (models included) and 6
evaluators. `AllowlistCoverageSuite` checks this against vendored copies of those
files, so a missing class fails the unit tests.

| Family | Operators |
| ------ | --------- |
| Classification | `LogisticRegression`, `LinearSVC`, `NaiveBayes`, `DecisionTreeClassifier`, `RandomForestClassifier`, `GBTClassifier`, `FMClassifier`, `MultilayerPerceptronClassifier`, `OneVsRest` |
| Regression | `LinearRegression`, `DecisionTreeRegressor`, `RandomForestRegressor`, `GBTRegressor`, `FMRegressor`, `AFTSurvivalRegression`, `IsotonicRegression`, `GeneralizedLinearRegression` |
| Clustering | `KMeans`, `BisectingKMeans`, `GaussianMixture`, `LDA` (local and distributed models), `PowerIterationClustering` |
| Recommendation | `ALS` |
| Frequent patterns | `FPGrowth`, `PrefixSpan` |
| Scalers and encoders | `StandardScaler`, `MinMaxScaler`, `MaxAbsScaler`, `RobustScaler`, `StringIndexer`, `OneHotEncoder`, `VectorIndexer`, `TargetEncoder`, `QuantileDiscretizer`, `Imputer` |
| Text and counts | `Tokenizer`, `RegexTokenizer`, `StopWordsRemover`, `NGram`, `HashingTF`, `CountVectorizer`, `IDF`, `Word2Vec` |
| Feature selection | `ChiSqSelector`, `UnivariateFeatureSelector`, `VarianceThresholdSelector`, `PCA`, `RFormula` |
| LSH | `BucketedRandomProjectionLSH`, `MinHashLSH` |
| Vector and feature transformers | `VectorAssembler`, `VectorSlicer`, `VectorSizeHint`, `Interaction`, `FeatureHasher`, `Binarizer`, `Bucketizer`, `Normalizer`, `PolynomialExpansion`, `DCT`, `ElementwiseProduct`, `IndexToString`, `SQLTransformer` |
| Evaluators | `RegressionEvaluator`, `BinaryClassificationEvaluator`, `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, `MultilabelClassificationEvaluator` |
| Tuning and pipelines | `ParamGridBuilder`, `CrossValidator`, `TrainValidationSplit`, `Pipeline` (and their models) |

Model attributes, methods and summaries are wired from the server's allowlist
(`MLUtils.invokeMethodAllowed`): for example `coefficients`, `featureImportances`,
`trees`, `treeWeights`, `toDebugString`, `predictLeaf`, `evaluate`,
`evaluateEachIteration`, `topicsMatrix`, `describeTopics`, `logLikelihood`,
`approxNearestNeighbors`, `approxSimilarityJoin`, `findSynonyms`, `getVectors`,
`selectedFeatures`, `categoryMapsDF`, and `summary`.

Integration tests cover each family against a live 4.3.0 server.

## Limitations

- Models are released explicitly with `release()`. The client does not release them
  when they are garbage collected.
- `OneVsRest` is not on the server allowlist. The client runs one binary fit per
  class on the server and does the argmax itself, as PySpark does over Connect.
- Tree models returned by `trees` are handles to the server's objects with generated
  client uids.
- `LinearSVC.evaluate` and the other `evaluate` methods return the generic
  `ClassificationSummary` (or `LinearRegressionTrainingSummary` and
  `GeneralizedLinearRegressionTrainingSummary`), not Spark's per-model summary types.
- `LDAModel.getCheckpointFiles` is not wrapped.
- `PowerIterationClustering.assignClusters` and `PrefixSpan.findFrequentSequentialPatterns`
  run through the server's ML helper, as PySpark does, so they are not estimators.

## Differences from Spark

- Models are held on the server. `model.release()` evicts a model; the client does
  not release models when they are garbage collected.
- Tuning runs `parallelism` fits at once, with the same seeded `rand` split as PySpark.
- `StopWordsRemover` defaults `stopWords` to Spark's English list, read from the server
  when the default is needed.
- `Summarizer` aliases its results by metric name (`mean`, `sum`, ...), not by the
  column expression.
- Vector and matrix columns decode to `Vector` and `Matrix` values in collected rows,
  and `show()` renders them as `[1.0,2.0]`.
