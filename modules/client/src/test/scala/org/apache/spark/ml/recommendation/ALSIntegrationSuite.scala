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
package org.apache.spark.ml.recommendation

import org.apache.spark.ml.MlTestData
import org.apache.spark.ml.MlRemoteSuite

class ALSIntegrationSuite extends MlRemoteSuite {

  test("ALS fits factors and recommends items for every user") {
    withSpark { spark =>
      val df = MlTestData.ratings(spark)
      val model = new ALS()
        .setRank(3)
        .setMaxIter(2)
        .setUserCol("user")
        .setItemCol("item")
        .setRatingCol("rating")
        .setColdStartStrategy("drop")
        .setSeed(1L)
        .fit(df)
      assertEquals(model.userFactors.count(), 6L)
      assert(model.itemFactors.count() > 0L)
      assertEquals(model.recommendForAllUsers(2).count(), 6L)
      assert(model.recommendForAllItems(2).count() > 0L)
      assert(model.transform(df).count() > 0L)
      model.release()
    }
  }
}
