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
package org.apache.spark.ml

import org.apache.spark.sql.{RemoteSparkSuite, SparkSession}

/**
 * Base for ML integration suites. ML Connect needs a Spark Connect server 4.0 or newer, so the
 * bodies run only against such a server and are skipped quietly on older ones (Spark 3.5 has no ML
 * Connect). Features that need a later server (such as summary recovery in 4.1) check their own
 * version.
 */
abstract class MlRemoteSuite extends RemoteSparkSuite {

  override protected def withSpark(body: SparkSession => Unit): Unit =
    if (serverAtLeast(4, 0)) body(spark)
}
