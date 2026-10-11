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

import org.apache.spark.sql.connect.client.SparkConnectClient

class SparkSessionSuite extends munit.FunSuite:

  test("closing a new session leaves the parent channel open"):
    val parent = SparkSession.builder.create()
    try
      val child = parent.newSession()
      child.close()
      assert(child.client.channel.isShutdown)
      assert(!parent.client.channel.isShutdown)
    finally parent.close()

  test("closing the parent leaves the new session channel open"):
    val parent = SparkSession.builder.create()
    val child = parent.newSession()
    try
      parent.close()
      assert(parent.client.channel.isShutdown)
      assert(!child.client.channel.isShutdown)
    finally child.close()

  test("new sessions keep client configuration with a fresh session ID"):
    val configuration = SparkConnectClient.Configuration(
      host = "example.invalid",
      port = 12345,
      useSsl = true,
      token = Some("test-token"),
      userId = Some("test-user"),
      userName = Some("test-name"),
      userAgent = "test-agent",
      sessionId = Some("parent-session"),
      metadata = Map("test-header" -> "test-value"),
      maxInboundMessageSize = 1024,
      useReattachableExecute = false,
      retryPolicies = Seq.empty
    )
    val parent = configuration.toSparkConnectClient
    try
      val child = parent.copy()
      try
        assertEquals(child.configuration, configuration.copy(sessionId = None))
        assertNotEquals(child.sessionId, parent.sessionId)
        assert(!(child.channel eq parent.channel))
      finally child.shutdown()
    finally parent.shutdown()
