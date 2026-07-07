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

package examples

import com.google.protobuf.any.{Any => ProtoAny}

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession

/**
 * Demonstrates the Spark Connect plugin extension point: building a `Relation.extension` from a
 * packed protobuf message and turning it into a DataFrame. This is the Scala counterpart of the
 * PySpark client's `plan.extension.Pack(msg)` + `DataFrame(plan, session)` and targets server-side
 * `org.apache.spark.sql.connect.plugin.RelationPlugin` implementations (e.g. GraphFrames).
 *
 * A real plugin packs its OWN generated message (from its own .proto), and the server must have the
 * matching `RelationPlugin` registered (via `spark.connect.extensions.relation.classes`) to unpack
 * and plan it. This example stands in with `proto.Range` -- any ScalaPB `GeneratedMessage` can be
 * packed -- purely to show the client-side plan construction, which needs no plugin installed.
 *
 * Note that the server ALWAYS routes an `extension` relation to a plugin; it does not interpret the
 * packed message as a built-in relation. So executing this DataFrame against a stock server (no
 * plugin registered) fails with "No handler found for extension" -- which is exactly the code path
 * a real plugin hooks into. This example therefore demonstrates and verifies the client side, and
 * shows (guarded) what execution looks like once a plugin is present.
 */
object RelationExtension {
  def main(args: Array[String]): Unit = {
    val remote = if (args.nonEmpty) args(0) else "sc://localhost:15002"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    try {
      // 1. Build a plugin message. A plugin can embed existing DataFrames by referencing their
      //    `.relation` (the protobuf logical plan) -- the counterpart of PySpark's
      //    `dataframe_to_proto(df, session)`.
      val pluginMessage = proto.Range(start = Some(0L), end = 5L, step = 1L)

      // 2. Pack it into a google.protobuf.Any and wrap it in a DataFrame via the extension point.
      val df = spark.newDataFrame(ProtoAny.pack(pluginMessage))

      // 3. The client-side plan is fully formed: an Extension relation carrying the packed message.
      //    This is everything the client is responsible for, and it needs no server round-trip.
      println("== extension relation plan ==")
      println(s"relType.isExtension = ${df.relation.relType.isExtension}")
      println(s"packed type url     = ${df.relation.getExtension.typeUrl}")
      require(df.relation.relType.isExtension, "expected an Extension relation")

      // 4. Execution dispatches to a server-side RelationPlugin. With a real plugin registered for
      //    this message type, `df.collect()` / `df.show()` return the plugin's rows. Against a stock
      //    server there is no handler, so we expect and report that instead of crashing.
      println("== execution (requires a registered server-side RelationPlugin) ==")
      try
        df.show()
      catch {
        case e: Exception =>
          println(s"No plugin registered for ${df.relation.getExtension.typeUrl}: ${e.getMessage}")
          println(
            "Register a RelationPlugin on the server (e.g. GraphFrames) to plan this message."
          )
      }
    } finally spark.stop()
  }
}
