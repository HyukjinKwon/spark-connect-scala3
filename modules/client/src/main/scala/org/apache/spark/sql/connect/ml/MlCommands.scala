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
package org.apache.spark.sql.connect.ml

import org.apache.spark.connect.proto

/**
 * Builds the ML Connect protos for each operation, without a session. The shapes follow the PySpark
 * client (`pyspark/ml/util.py`, `pyspark/ml/connect/readwrite.py`), so the server sees the same
 * requests. [[MlClient]] sends them.
 */
private[spark] object MlCommands {

  /** `Fit`: fits `estimator` on `dataset`. */
  def fit(
      estimator: proto.MlOperator,
      params: proto.MlParams,
      dataset: proto.Relation
  ): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.Fit(
        proto.MlCommand
          .Fit(estimator = Some(estimator), params = Some(params), dataset = Some(dataset))
      )
    )

  /** `Evaluate`: scores `dataset` with `evaluator`. */
  def evaluate(
      evaluator: proto.MlOperator,
      params: proto.MlParams,
      dataset: proto.Relation
  ): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.Evaluate(
        proto.MlCommand
          .Evaluate(evaluator = Some(evaluator), params = Some(params), dataset = Some(dataset))
      )
    )

  /** `Fetch`: reads an attribute, or calls a method, of a cached object. */
  def fetch(objRef: String, methods: Seq[proto.Fetch.Method]): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.Fetch(
        proto.Fetch(objRef = Some(proto.ObjectRef(objRef)), methods = methods)
      )
    )

  /** An `MlRelation` that fetches an attribute which is a DataFrame. */
  def fetchRelation(
      objRef: String,
      methods: Seq[proto.Fetch.Method],
      summaryDataset: Option[proto.Relation]
  ): proto.MlRelation =
    proto.MlRelation(
      mlType = proto.MlRelation.MlType
        .Fetch(proto.Fetch(objRef = Some(proto.ObjectRef(objRef)), methods = methods)),
      modelSummaryDataset = summaryDataset
    )

  /** An `MlRelation` for `transform(input)` with a cached model (`Left`) or a new transformer. */
  def transform(
      operator: Either[String, proto.MlOperator],
      params: proto.MlParams,
      input: proto.Relation
  ): proto.MlRelation = {
    val transform = operator match {
      case Left(objRef) =>
        proto.MlRelation.Transform(
          operator = proto.MlRelation.Transform.Operator.ObjRef(proto.ObjectRef(objRef)),
          input = Some(input),
          params = Some(params)
        )
      case Right(op) =>
        proto.MlRelation.Transform(
          operator = proto.MlRelation.Transform.Operator.Transformer(op),
          input = Some(input),
          params = Some(params)
        )
    }
    proto.MlRelation(mlType = proto.MlRelation.MlType.Transform(transform))
  }

  /** `Write`: saves a cached model or an operator. */
  def write(write: proto.MlCommand.Write): proto.MlCommand =
    proto.MlCommand(command = proto.MlCommand.Command.Write(write))

  /** `Read`: loads an operator of `name` and `operatorType` from `path`. */
  def read(
      name: String,
      operatorType: proto.MlOperator.OperatorType,
      path: String
  ): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.Read(
        proto.MlCommand
          .Read(operator = Some(proto.MlOperator(name = name, `type` = operatorType)), path = path)
      )
    )

  /** `Delete`: evicts cached objects by id. */
  def delete(ids: Seq[String]): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.Delete(proto.MlCommand.Delete(objRefs = ids.map(proto.ObjectRef(_))))
    )

  /** `CleanCache`: evicts every cached object of the session. */
  def cleanCache: proto.MlCommand =
    proto.MlCommand(command = proto.MlCommand.Command.CleanCache(proto.MlCommand.CleanCache()))

  /** `GetModelSize`: the estimated in-memory size of a cached model. */
  def getModelSize(modelRef: String): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.GetModelSize(
        proto.MlCommand.GetModelSize(modelRef = Some(proto.ObjectRef(modelRef)))
      )
    )

  /** `GetCacheInfo`: a description of the ML objects cached in the session. */
  def getCacheInfo: proto.MlCommand =
    proto.MlCommand(command = proto.MlCommand.Command.GetCacheInfo(proto.MlCommand.GetCacheInfo()))

  /** `CreateSummary`: recreates a model summary that was lost from the cache. */
  def createSummary(modelRef: String, dataset: proto.Relation): proto.MlCommand =
    proto.MlCommand(command =
      proto.MlCommand.Command.CreateSummary(
        proto.MlCommand
          .CreateSummary(modelRef = Some(proto.ObjectRef(modelRef)), dataset = Some(dataset))
      )
    )

  /** The `Command` plan that carries an ML command. */
  def plan(command: proto.MlCommand): proto.Plan =
    proto.Plan(
      proto.Plan.OpType.Command(
        proto.Command(commandType = proto.Command.CommandType.MlCommand(command))
      )
    )
}
