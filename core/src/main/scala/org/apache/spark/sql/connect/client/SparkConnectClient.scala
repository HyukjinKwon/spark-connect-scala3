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

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.grpc.{
  CallOptions,
  Channel,
  ClientCall,
  ClientInterceptor,
  ClientInterceptors,
  ManagedChannel,
  ManagedChannelBuilder,
  Metadata,
  MethodDescriptor,
  Status,
  StatusRuntimeException
}
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.SparkConnectServiceGrpc

/**
 * The low-level Spark Connect client. Wraps the gRPC blocking stub and exposes the core RPC
 * families used by the higher level API ([[org.apache.spark.sql.SparkSession]] and
 * [[org.apache.spark.sql.Dataset]]): `execute`, `analyze`, `config`, and `interrupt`.
 *
 * Transient transport failures (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, ...) are retried with
 * exponential backoff and jitter before any response data has been observed.
 *
 * Construct one with [[SparkConnectClient.builder]] or [[SparkConnectClient.apply(String)]].
 */
class SparkConnectClient private[client] (
    val configuration: SparkConnectClient.Configuration,
    private val channel: ManagedChannel
) {

  import SparkConnectClient._

  private val interceptedChannel: Channel =
    if (configuration.metadata.isEmpty) channel
    else ClientInterceptors.intercept(channel, new HeaderClientInterceptor(configuration.metadata))

  private val stub: SparkConnectServiceGrpc.SparkConnectServiceBlockingStub =
    SparkConnectServiceGrpc.blockingStub(interceptedChannel)

  /** The client-generated session id (UUID v4) that scopes all requests. */
  val sessionId: String = configuration.sessionId.getOrElse(UUID.randomUUID().toString)

  /** The client type / user agent reported to the server. */
  val clientType: String = configuration.userAgent

  private val userContext: proto.UserContext =
    proto.UserContext(userId = configuration.userId.getOrElse(""))

  @volatile private var serverSideSessionId: Option[String] = None
  @volatile private var closed: Boolean = false
  private val retry = configuration.retryPolicy

  // ---------------------------------------------------------------------------
  // RPCs
  // ---------------------------------------------------------------------------

  /**
   * Execute a [[proto.Plan]] and return the streamed responses as a Java iterator. The stream is
   * lazy: the RPC is issued on first access. Higher layers wrap this in a [[SparkResult]] to
   * materialize rows.
   */
  def execute(plan: proto.Plan): java.util.Iterator[proto.ExecutePlanResponse] = {
    checkOpen()
    val request = proto.ExecutePlanRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      operationId = Some(UUID.randomUUID().toString),
      plan = Some(plan),
      clientType = Some(clientType),
      tags = tags.toSeq
    )
    val it = retry.retry(s"execute")(stub.executePlan(request))
    it.map { response =>
      if (response.serverSideSessionId.nonEmpty) {
        serverSideSessionId = Some(response.serverSideSessionId)
      }
      response
    }.asJava
  }

  /** Run an `AnalyzePlan` request; identity fields (session/user/client) are injected here. */
  def analyze(request: proto.AnalyzePlanRequest): proto.AnalyzePlanResponse = {
    checkOpen()
    val req = request.copy(
      sessionId = sessionId,
      userContext = Some(userContext),
      clientType = Some(clientType)
    )
    retry.retry("analyze")(stub.analyzePlan(req))
  }

  /** Convenience: build an `AnalyzePlan` request from just the `analyze` oneof. */
  def analyze(analyze: proto.AnalyzePlanRequest.Analyze): proto.AnalyzePlanResponse =
    this.analyze(proto.AnalyzePlanRequest(analyze = analyze))

  /** Run a `Config` request. */
  def config(operation: proto.ConfigRequest.Operation): proto.ConfigResponse = {
    checkOpen()
    val req = proto.ConfigRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      clientType = Some(clientType),
      operation = Some(operation)
    )
    retry.retry("config")(stub.config(req))
  }

  /** Interrupt all running operations of this session. */
  def interruptAll(): proto.InterruptResponse =
    interrupt(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)

  /** Interrupt operations tagged with `tag`. */
  def interruptTag(tag: String): proto.InterruptResponse =
    interrupt(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG, _.withOperationTag(tag))

  /** Interrupt the operation with id `operationId`. */
  def interruptOperation(operationId: String): proto.InterruptResponse =
    interrupt(
      proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID,
      _.withOperationId(operationId)
    )

  private def interrupt(
      tpe: proto.InterruptRequest.InterruptType,
      f: proto.InterruptRequest => proto.InterruptRequest = identity
  ): proto.InterruptResponse = {
    checkOpen()
    val req = f(
      proto.InterruptRequest(
        sessionId = sessionId,
        userContext = Some(userContext),
        clientType = Some(clientType),
        interruptType = tpe
      )
    )
    retry.retry("interrupt")(stub.interrupt(req))
  }

  /** Release this client's server-side session. Best-effort, never throws. */
  def releaseSession(): Unit = {
    if (closed) return
    try
      stub.releaseSession(
        proto.ReleaseSessionRequest(
          sessionId = sessionId,
          userContext = Some(userContext),
          clientType = Some(clientType)
        )
      )
    catch {
      case NonFatal(_) => // teardown is best-effort
    }
  }

  // ---------------------------------------------------------------------------
  // Operation tags
  // ---------------------------------------------------------------------------

  private val tags = scala.collection.mutable.LinkedHashSet.empty[String]

  def addTag(tag: String): Unit = {
    require(tag.nonEmpty, "Tag must not be empty")
    require(!tag.contains(','), "Tag must not contain ','")
    tags.synchronized(tags += tag)
  }
  def removeTag(tag: String): Unit = tags.synchronized(tags -= tag)
  def clearTags(): Unit = tags.synchronized(tags.clear())
  def getTags: Set[String] = tags.synchronized(tags.toSet)

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  def isSessionValid: Boolean = !closed && !channel.isShutdown

  /** A new client sharing the same configuration but a fresh session id. */
  def copy(): SparkConnectClient =
    SparkConnectClient.fromConfiguration(configuration.copy(sessionId = None))

  /** Shut down the gRPC channel. Idempotent. */
  def shutdown(): Unit = {
    closed = true
    try {
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
    } catch {
      case NonFatal(_) =>
    }
  }

  private def checkOpen(): Unit =
    if (closed) throw new IllegalStateException("Spark Connect client has been shut down")
}

object SparkConnectClient {

  /** The environment variable read by [[Builder.loadFromEnvironment]]. */
  val SPARK_REMOTE: String = "SPARK_REMOTE"
  val DEFAULT_PORT: Int = 15002

  def builder(): Builder = new Builder()

  /** Build a client directly from an `sc://` connection string. */
  def apply(connectionString: String): SparkConnectClient =
    builder().connectionString(connectionString).build()

  private[sql] def fromConfiguration(configuration: Configuration): SparkConnectClient = {
    val builder = ManagedChannelBuilder
      .forAddress(configuration.host, configuration.port)
      .userAgent(configuration.userAgent)
      .maxInboundMessageSize(configuration.maxInboundMessageSize)
    if (configuration.useSsl) builder.useTransportSecurity() else builder.usePlaintext()
    new SparkConnectClient(configuration, builder.build())
  }

  // ---------------------------------------------------------------------------
  // Configuration + connection-string parsing
  // ---------------------------------------------------------------------------

  /**
   * Immutable connection configuration, typically produced by parsing an `sc://` connection string.
   * Mirrors the grammar of the official Spark Connect clients.
   */
  final case class Configuration(
      host: String = "localhost",
      port: Int = DEFAULT_PORT,
      token: Option[String] = None,
      useSsl: Boolean = false,
      userId: Option[String] = None,
      userAgent: String = DEFAULT_USER_AGENT,
      sessionId: Option[String] = None,
      maxInboundMessageSize: Int = 128 * 1024 * 1024,
      extraHeaders: Map[String, String] = Map.empty,
      retryPolicy: RetryPolicy = RetryPolicy()
  ) {

    /** Per-request gRPC metadata (bearer token + any `x-*` params). */
    def metadata: Map[String, String] = {
      val auth = token.map(t => "authorization" -> s"Bearer $t").toMap
      auth ++ extraHeaders
    }

    def toSparkConnectClient: SparkConnectClient = fromConfiguration(this)
  }

  /** Library version, surfaced in the gRPC user agent. */
  val VERSION: String = "0.1.0"

  val DEFAULT_USER_AGENT: String = s"spark-connect-scala3/$VERSION"

  /**
   * Parse a Spark Connect connection string of the form
   * {{{sc://host[:port][/;param=value;param=value...]}}}
   *
   * Recognised params: `token`, `user_id`, `user_agent`, `use_ssl`, `session_id`. Any param
   * beginning with `x-` is forwarded verbatim as gRPC request metadata. A bearer `token` implies
   * TLS.
   */
  def parseConnectionString(url: String): Configuration = {
    require(url != null, "Connection string must not be null")
    require(url.startsWith("sc://"), s"Connection string must start with 'sc://', got: $url")
    val body = url.stripPrefix("sc://")
    val slash = body.indexOf('/')
    val (endpoint, paramStr) =
      if (slash < 0) (body, "") else (body.substring(0, slash), body.substring(slash + 1))

    val params = parseParams(paramStr)
    val (host, port) = parseEndpoint(endpoint)

    val token = params.get("token").filter(_.nonEmpty)
    val useSsl = params.get("use_ssl").map(parseBool).getOrElse(token.isDefined)
    val extraHeaders = params.collect {
      case (k, v) if k.startsWith("x-") => k -> v
    }
    Configuration(
      host = host,
      port = port,
      token = token,
      useSsl = useSsl,
      userId = params.get("user_id").filter(_.nonEmpty),
      userAgent = params.getOrElse("user_agent", DEFAULT_USER_AGENT),
      sessionId = params.get("session_id").filter(_.nonEmpty),
      extraHeaders = extraHeaders
    )
  }

  private def parseEndpoint(endpoint: String): (String, Int) = {
    require(endpoint.nonEmpty, "Missing host in connection string")
    val idx = endpoint.lastIndexOf(':')
    if (idx < 0) {
      (endpoint, DEFAULT_PORT)
    } else {
      val host = endpoint.substring(0, idx)
      val portStr = endpoint.substring(idx + 1)
      val port = portStr.toIntOption.getOrElse(
        throw new IllegalArgumentException(s"Invalid port in connection string: $portStr")
      )
      (host, port)
    }
  }

  private def parseParams(paramStr: String): Map[String, String] =
    paramStr
      .split(';')
      .filter(_.nonEmpty)
      .map { kv =>
        val eq = kv.indexOf('=')
        require(eq > 0, s"Malformed parameter (expected key=value): $kv")
        decode(kv.substring(0, eq)) -> decode(kv.substring(eq + 1))
      }
      .toMap

  private def decode(s: String): String =
    java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8)

  private def parseBool(s: String): Boolean =
    Set("true", "1", "yes").contains(s.toLowerCase(java.util.Locale.ROOT))

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  class Builder {
    private var configuration: Configuration = Configuration()

    def configuration(configuration: Configuration): this.type = {
      this.configuration = configuration
      this
    }

    def connectionString(url: String): this.type = {
      this.configuration = parseConnectionString(url)
      this
    }

    def host(host: String): this.type = { configuration = configuration.copy(host = host); this }
    def port(port: Int): this.type = { configuration = configuration.copy(port = port); this }
    def token(token: String): this.type = {
      configuration = configuration.copy(token = Some(token), useSsl = true); this
    }
    def userId(userId: String): this.type = {
      configuration = configuration.copy(userId = Some(userId)); this
    }
    def userAgent(userAgent: String): this.type = {
      configuration = configuration.copy(userAgent = userAgent); this
    }
    def sessionId(sessionId: String): this.type = {
      configuration = configuration.copy(sessionId = Some(sessionId)); this
    }
    def useSsl(useSsl: Boolean): this.type = {
      configuration = configuration.copy(useSsl = useSsl); this
    }

    /** Initialise the connection string from the `SPARK_REMOTE` env var, if set. */
    def loadFromEnvironment(): this.type = {
      sys.env
        .get(SPARK_REMOTE)
        .orElse(sys.props.get("spark.remote"))
        .foreach(connectionString)
      this
    }

    def build(): SparkConnectClient = fromConfiguration(configuration)
  }

  // ---------------------------------------------------------------------------
  // Retry
  // ---------------------------------------------------------------------------

  /** Exponential-backoff retry policy for transient gRPC failures. */
  final case class RetryPolicy(
      maxRetries: Int = 10,
      baseDelayMs: Long = 50L,
      maxDelayMs: Long = 10000L
  ) {

    private val retryableCodes: Set[Status.Code] = Set(
      Status.Code.UNAVAILABLE,
      Status.Code.DEADLINE_EXCEEDED,
      Status.Code.ABORTED,
      Status.Code.RESOURCE_EXHAUSTED
    )

    def retry[T](op: String)(body: => T): T = {
      var attempt = 0
      while (true)
        try
          return body
        catch {
          case e: StatusRuntimeException
              if retryableCodes.contains(e.getStatus.getCode) && attempt < maxRetries =>
            val base = math.min(baseDelayMs * (1L << attempt), maxDelayMs).toDouble
            val jitter = (math.random() * base * 0.5).toLong
            Thread.sleep(base.toLong + jitter)
            attempt += 1
          case e: StatusRuntimeException =>
            throw SparkConnectException.from(e)
        }
      throw new IllegalStateException(s"unreachable retry state for $op")
    }
  }

  // ---------------------------------------------------------------------------
  // Header interceptor
  // ---------------------------------------------------------------------------

  private class HeaderClientInterceptor(headers: Map[String, String]) extends ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel
    ): ClientCall[ReqT, RespT] =
      new SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {
        override def start(
            responseListener: ClientCall.Listener[RespT],
            requestHeaders: Metadata
        ): Unit = {
          headers.foreach { case (k, v) =>
            requestHeaders.put(Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER), v)
          }
          super.start(responseListener, requestHeaders)
        }
      }
  }
}
