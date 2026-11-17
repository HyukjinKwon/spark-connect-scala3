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
package org.apache.spark.ml.connect

import org.apache.spark.connect.proto
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.connect.ml.MlCommands

/** Checks the ML Connect commands and relations built for each operation, without a server. */
class MlCommandsSuite extends munit.FunSuite {

  private val relation = proto.Relation(relType =
    proto.Relation.RelType.Range(proto.Range(start = Some(0L), end = 3L, step = 1L))
  )

  test("fit sends an estimator operator with its params and the dataset") {
    val lr = new LogisticRegression().setMaxIter(4).setRegParam(0.1)
    val estimator = RemoteOps.operator(
      LogisticRegression.OperatorName,
      lr.uid,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR
    )
    val command = MlCommands.fit(estimator, MlSerializer.encodeParams(lr), relation)
    val fit = command.getFit
    assertEquals(fit.getEstimator.name, "org.apache.spark.ml.classification.LogisticRegression")
    assertEquals(fit.getEstimator.uid, lr.uid)
    assertEquals(fit.getEstimator.`type`, proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)
    assertEquals(fit.getParams.params.keySet, Set("maxIter", "regParam"))
    assertEquals(fit.getDataset, relation)
  }

  test("the command is wrapped in a Command plan") {
    val plan = MlCommands.plan(MlCommands.cleanCache)
    assert(plan.getCommand.getMlCommand.command.isCleanCache)
  }

  test("evaluate sends an evaluator operator") {
    val evaluator = RemoteOps.operator(
      "org.apache.spark.ml.evaluation.RegressionEvaluator",
      "regEval_1",
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR
    )
    val command = MlCommands.evaluate(evaluator, proto.MlParams(), relation)
    assertEquals(
      command.getEvaluate.getEvaluator.`type`,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR
    )
    assertEquals(command.getEvaluate.getDataset, relation)
  }

  test("transform on a cached model references the object, not a transformer") {
    val rel = MlCommands.transform(Left("model_42"), proto.MlParams(), relation)
    val transform = rel.getTransform
    assertEquals(transform.getObjRef.id, "model_42")
    assert(transform.operator.isObjRef)
    assertEquals(transform.getInput, relation)
  }

  test("transform with a new transformer sends the operator") {
    val op = RemoteOps.operator(
      "org.apache.spark.ml.feature.Tokenizer",
      "tok_1",
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER
    )
    val rel = MlCommands.transform(Right(op), proto.MlParams(), relation)
    assertEquals(rel.getTransform.getTransformer.name, "org.apache.spark.ml.feature.Tokenizer")
    assert(rel.getTransform.operator.isTransformer)
  }

  test("fetch sends method calls with their arguments") {
    val arg = proto.Fetch.Method.Args(argsType =
      proto.Fetch.Method.Args.ArgsType.Param(MlSerializer.encodeValue(Vectors.dense(1.0, 2.0)))
    )
    val command =
      MlCommands.fetch("model_1", Seq(proto.Fetch.Method(method = "predict", args = Seq(arg))))
    assertEquals(command.getFetch.getObjRef.id, "model_1")
    assertEquals(command.getFetch.methods.head.method, "predict")
    assert(command.getFetch.methods.head.args.head.argsType.isParam)
  }

  test("fetchRelation carries the summary dataset for lost-summary recovery") {
    val rel = MlCommands.fetchRelation(
      "model_1",
      Seq(proto.Fetch.Method(method = "summary"), proto.Fetch.Method(method = "predictions")),
      Some(relation)
    )
    assertEquals(rel.getFetch.getObjRef.id, "model_1")
    assertEquals(rel.getFetch.methods.map(_.method), Seq("summary", "predictions"))
    assertEquals(rel.getModelSummaryDataset, relation)
  }

  test("write of a cached model carries its reference, params, path and overwrite flag") {
    val write = proto.MlCommand.Write(
      `type` = proto.MlCommand.Write.Type.ObjRef(proto.ObjectRef("model_9")),
      params = Some(proto.MlParams()),
      path = "/tmp/m",
      shouldOverwrite = Some(true),
      options = Map("k" -> "v")
    )
    val command = MlCommands.write(write)
    assertEquals(command.getWrite.getObjRef.id, "model_9")
    assertEquals(command.getWrite.path, "/tmp/m")
    assert(command.getWrite.getShouldOverwrite)
    assertEquals(command.getWrite.options, Map("k" -> "v"))
  }

  test("read names the operator type and path") {
    val command = MlCommands.read(
      "org.apache.spark.ml.classification.LogisticRegression",
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      "/tmp/lr"
    )
    assertEquals(
      command.getRead.getOperator.name,
      "org.apache.spark.ml.classification.LogisticRegression"
    )
    assertEquals(
      command.getRead.getOperator.`type`,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR
    )
    assertEquals(command.getRead.path, "/tmp/lr")
  }

  test("delete and createSummary reference objects by id") {
    assertEquals(MlCommands.delete(Seq("a", "b")).getDelete.objRefs.map(_.id), Seq("a", "b"))
    val summary = MlCommands.createSummary("model_3", relation)
    assertEquals(summary.getCreateSummary.getModelRef.id, "model_3")
    assertEquals(summary.getCreateSummary.getDataset, relation)
  }
}
