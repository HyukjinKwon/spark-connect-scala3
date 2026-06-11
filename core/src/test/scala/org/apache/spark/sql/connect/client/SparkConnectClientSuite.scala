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

import SparkConnectClient.parseConnectionString

class SparkConnectClientSuite extends munit.FunSuite {

  test("default host/port and plaintext") {
    val c = parseConnectionString("sc://localhost")
    assertEquals(c.host, "localhost")
    assertEquals(c.port, 15002)
    assertEquals(c.useSsl, false)
    assertEquals(c.token, None)
  }

  test("explicit port") {
    val c = parseConnectionString("sc://example.com:443")
    assertEquals(c.host, "example.com")
    assertEquals(c.port, 443)
  }

  test("params: user_id, session_id, use_ssl, user_agent") {
    val c = parseConnectionString(
      "sc://host:15002/;user_id=alice;session_id=abc;use_ssl=true;user_agent=my-app"
    )
    assertEquals(c.userId, Some("alice"))
    assertEquals(c.sessionId, Some("abc"))
    assertEquals(c.useSsl, true)
    assertEquals(c.userAgent, "my-app")
  }

  test("token implies TLS and an authorization header") {
    val c = parseConnectionString("sc://host/;token=secret")
    assertEquals(c.token, Some("secret"))
    assertEquals(c.useSsl, true)
    assertEquals(c.metadata.get("authorization"), Some("Bearer secret"))
  }

  test("x- params are forwarded as metadata") {
    val c = parseConnectionString("sc://host/;x-custom=value;user_id=bob")
    assertEquals(c.metadata.get("x-custom"), Some("value"))
    assert(!c.metadata.contains("user_id"))
  }

  test("rejects connection strings without the sc:// scheme") {
    intercept[IllegalArgumentException](parseConnectionString("http://localhost"))
  }

  test("rejects an invalid port") {
    intercept[IllegalArgumentException](parseConnectionString("sc://host:notaport"))
  }

  test("rejects a malformed parameter") {
    intercept[IllegalArgumentException](parseConnectionString("sc://host/;bogus"))
  }
}
