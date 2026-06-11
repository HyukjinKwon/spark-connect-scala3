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

package org.apache.spark.sql.connect.client

import org.apache.spark.sql.connect.client.SparkConnectClient.Configuration

/**
 * Unit tests for [[SparkConnectClient.parseConnectionString]]. These run entirely offline (no
 * gRPC channel, no server) and assert on the resulting [[Configuration]] case class.
 */
class ConnectionStringSuite extends munit.FunSuite {

  private val base = Configuration()

  private def parse(s: String): Configuration =
    SparkConnectClient.parseConnectionString(s, base)

  test("host and explicit port") {
    val c = parse("sc://example.com:1234")
    assertEquals(c.host, "example.com")
    assertEquals(c.port, 1234)
    assertEquals(c.useSsl, false)
  }

  test("host without port keeps the default port") {
    val c = parse("sc://myhost")
    assertEquals(c.host, "myhost")
    assertEquals(c.port, Configuration.DEFAULT_PORT)
    assertEquals(c.port, 15002)
  }

  test("localhost with default port") {
    val c = parse("sc://localhost:15002")
    assertEquals(c.host, "localhost")
    assertEquals(c.port, 15002)
  }

  test("parameters: user_id and token") {
    val c = parse("sc://host:15002/;user_id=alice;token=secret")
    assertEquals(c.userId, Some("alice"))
    assertEquals(c.token, Some("secret"))
  }

  test("token implies ssl") {
    val c = parse("sc://host:15002/;token=secret")
    assertEquals(c.token, Some("secret"))
    assertEquals(c.useSsl, true)
  }

  test("explicit use_ssl=true") {
    val c = parse("sc://host:15002/;use_ssl=true")
    assertEquals(c.useSsl, true)
  }

  test("use_ssl=false by default") {
    val c = parse("sc://host:15002")
    assertEquals(c.useSsl, false)
  }

  test("session_id and user_agent parameters") {
    val c = parse("sc://host:15002/;session_id=abc-123;user_agent=my-agent")
    assertEquals(c.sessionId, Some("abc-123"))
    assertEquals(c.userAgent, "my-agent")
  }

  test("unknown parameters land in metadata") {
    val c = parse("sc://host:15002/;custom_key=custom_value")
    assertEquals(c.metadata.get("custom_key"), Some("custom_value"))
  }

  test("missing sc:// prefix throws") {
    intercept[IllegalArgumentException] {
      parse("http://host:15002")
    }
  }

  test("bare host:port without prefix throws") {
    intercept[IllegalArgumentException] {
      parse("host:15002")
    }
  }

  test("non-numeric port throws") {
    intercept[IllegalArgumentException] {
      parse("sc://host:notaport")
    }
  }

  test("malformed parameter (no '=') throws") {
    intercept[IllegalArgumentException] {
      parse("sc://host:15002/;justakey")
    }
  }

  test("Configuration default fields") {
    val c = Configuration()
    assertEquals(c.host, "localhost")
    assertEquals(c.port, 15002)
    assertEquals(c.useSsl, false)
    assertEquals(c.token, None)
    assertEquals(c.sessionId, None)
    assertEquals(c.userAgent, Configuration.DEFAULT_USER_AGENT)
  }
}
