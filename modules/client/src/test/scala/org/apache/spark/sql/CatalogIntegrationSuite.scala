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
package org.apache.spark.sql

/**
 * Breadth coverage for `spark.catalog`: catalog/database/function listing and lookup, plus the
 * temp-view and caching lifecycle. These APIs exist on every supported server (Spark 3.5+).
 */
class CatalogIntegrationSuite extends RemoteSparkSuite {

  test("catalog, database and function listing and lookup") {
    withSpark { s =>
      assert(s.catalog.listCatalogs().count() >= 1L)
      assert(s.catalog.listDatabases().count() >= 1L)
      assert(s.catalog.databaseExists("default"))
      assert(s.catalog.currentCatalog.nonEmpty)
      assert(s.catalog.currentDatabase.nonEmpty)
      assert(s.catalog.listFunctions().count() > 0L)
      assert(s.catalog.functionExists("abs"))
      assert(!s.catalog.functionExists("a_function_that_does_not_exist_scs3"))
    }
  }

  test("temp view: columns, existence, caching lifecycle, and drop") {
    withSpark { s =>
      val name = "sc3_catalog_view"
      s.range(0, 5).createOrReplaceTempView(name)
      try {
        assert(s.catalog.tableExists(name))
        assertEquals(s.catalog.listColumns(name).count(), 1L) // the `id` column

        s.catalog.cacheTable(name)
        assert(s.catalog.isCached(name))
        s.catalog.uncacheTable(name)
        assert(!s.catalog.isCached(name))
        s.catalog.clearCache()
      } finally s.catalog.dropTempView(name)

      assert(!s.catalog.tableExists(name))
    }
  }

  test("current database can be read and set back") {
    withSpark { s =>
      val original = s.catalog.currentDatabase
      s.catalog.setCurrentDatabase(original) // no-op, but exercises the RPC
      assertEquals(s.catalog.currentDatabase, original)
    }
  }
}
