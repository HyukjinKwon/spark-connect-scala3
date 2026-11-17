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
package org.apache.spark.ml.fpm

import org.apache.spark.ml.MlRemoteSuite

class PrefixSpanIntegrationSuite extends MlRemoteSuite {

  test("PrefixSpan mines frequent sequential patterns") {
    withSpark { spark =>
      val sequences = spark.sql(
        "SELECT array(array(1), array(2), array(3)) AS sequence UNION ALL " +
          "SELECT array(array(1), array(2)) AS sequence UNION ALL " +
          "SELECT array(array(1), array(3)) AS sequence"
      )
      val patterns = new PrefixSpan()
        .setMinSupport(0.5)
        .setMaxPatternLength(3)
        .findFrequentSequentialPatterns(sequences)
      assert(patterns.columns.contains("freq"))
      assert(patterns.count() > 0L)
    }
  }

  test("PrefixSpan defaults match Spark") {
    val ps = new PrefixSpan()
    assertEquals(ps.getMinSupport, 0.1)
    assertEquals(ps.getMaxPatternLength, 10)
    assertEquals(ps.getSequenceCol, "sequence")
  }
}
