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
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.ml.MlClient

/** A model whose fitted state lives on the server, behind a [[ModelRef]]. */
private[ml] trait RemoteModelLike {
  private[ml] def ref: ModelRef
}

/** Persistence for ML operators and cached models, through `MlCommand.Write` and `Read`. */
private[ml] object RemoteIO {

  /**
   * Saves `instance` to `path`. Cached models are saved by reference, and other operators are saved
   * by class name, uid, and their set parameters.
   */
  def save(
      instance: Params,
      path: String,
      overwrite: Boolean,
      options: Map[String, String]
  ): Unit = {
    val write = instance match {
      case m: RemoteModelLike =>
        proto.MlCommand.Write(
          `type` = proto.MlCommand.Write.Type.ObjRef(proto.ObjectRef(m.ref.id)),
          params = Some(MlSerializer.encodeParams(instance)),
          path = path,
          shouldOverwrite = Some(overwrite),
          options = options
        )
      case op => operatorWrite(op, path, overwrite, options)
    }
    val session = instance match {
      case m: RemoteModelLike => m.ref.session
      case _ => SparkSession.active
    }
    MlClient.write(session, write)
  }

  private def operatorWrite(
      op: Params,
      path: String,
      overwrite: Boolean,
      options: Map[String, String]
  ): proto.MlCommand.Write = {
    val operatorType = op match {
      case _: Estimator[_] => proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR
      case _: Evaluator => proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR
      case _: Transformer => proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER
      case other =>
        throw new IllegalArgumentException(s"${other.getClass.getName} is not writable.")
    }
    proto.MlCommand.Write(
      `type` = proto.MlCommand.Write.Type
        .Operator(RemoteOps.operator(op.getClass.getName, op.uid, operatorType)),
      params = Some(MlSerializer.encodeParams(op)),
      path = path,
      shouldOverwrite = Some(overwrite),
      options = options
    )
  }

  /** Loads an estimator, evaluator, or transformer of class `name` from `path`. */
  def loadOperator[T <: Params](
      name: String,
      operatorType: proto.MlOperator.OperatorType,
      path: String,
      construct: String => T
  ): T = {
    val session = SparkSession.active
    val info = MlClient.read(session, name, operatorType, path)
    val instance = construct(info.getUid)
    MlSerializer.applyParams(instance, info.getParams)
    instance
  }

  /** Loads a model of class `name` from `path` and caches it on the server. */
  def loadModel[T <: Params](name: String, path: String, construct: (String, ModelRef) => T): T = {
    val session = SparkSession.active
    val info = MlClient.read(session, name, proto.MlOperator.OperatorType.OPERATOR_TYPE_MODEL, path)
    val ref = new ModelRef(info.getObjRef.id, session, None)
    val instance = construct(info.getUid, ref)
    MlSerializer.applyParams(instance, info.getParams)
    instance
  }
}
