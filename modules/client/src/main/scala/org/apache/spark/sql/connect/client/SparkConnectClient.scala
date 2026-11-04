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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Base64, UUID}
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata => GrpcMetadata}
import io.grpc.stub.MetadataUtils

import org.apache.spark.connect.proto

/**
 * The low-level client that talks to a Spark Connect server over gRPC.
 *
 * It owns the gRPC channel, the (stable) session id, the user context and the user agent, and
 * exposes the RPCs used by the public API: `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`,
 * `ReleaseSession`, and `GetStatus`.
 */
class SparkConnectClient private[sql] (
    private[sql] val configuration: SparkConnectClient.Configuration,
    private[sql] val channel: ManagedChannel
) {

  import SparkConnectClient._

  private val stub = proto.SparkConnectServiceGrpc.blockingStub(channel)
  private val retryHandler = new GrpcRetryHandler(
    configuration.retryPolicies,
    Thread.sleep,
    configuration.maxRetryExceptionElapsedTime
  )
  private lazy val reattachableStub = new GrpcReattachableStub(
    channel,
    configuration.rpcDeadlines.reattachableExecutePlan,
    configuration.rpcDeadlines.reattachExecute
  )

  /**
   * A stub whose calls are bounded by `deadline`. Built per call: gRPC deadlines are absolute, so
   * each retry attempt must get a fresh budget.
   */
  private def stubWithDeadline(
      deadline: Option[FiniteDuration]
  ): proto.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub =
    deadline.fold(stub)(d => stub.withDeadlineAfter(d.toMillis, TimeUnit.MILLISECONDS))

  /** The stable session id shared by all requests issued through this client. */
  val sessionId: String = configuration.sessionId.getOrElse(UUID.randomUUID().toString)

  def userAgent: String = configuration.userAgent
  def host: String = configuration.host
  def port: Int = configuration.port

  private[sql] def userContext: proto.UserContext =
    proto.UserContext(
      userId = configuration.userId.getOrElse(""),
      userName = configuration.userName.getOrElse(configuration.userId.getOrElse(""))
    )

  // Converts gRPC errors into Spark exceptions, using the FetchErrorDetails RPC to reconstruct the
  // full server stack trace and cause chain when the server attaches an error id.
  private val exceptions = new GrpcExceptionConverter(fetchErrorDetails)

  /**
   * Fetches the server's structured error details for an error id. Uses the raw stub (never the
   * exception converter) so a failure here can't recurse; returns `None` on any problem.
   */
  private def fetchErrorDetails(errorId: String): Option[proto.FetchErrorDetailsResponse] =
    try
      Some(
        stubWithDeadline(configuration.rpcDeadlines.fetchErrorDetails).fetchErrorDetails(
          proto.FetchErrorDetailsRequest(
            sessionId = sessionId,
            userContext = Some(userContext),
            errorId = errorId,
            clientType = Some(userAgent)
          )
        )
      )
    catch { case scala.util.control.NonFatal(_) => None }

  // ---------------------------------------------------------------------------
  // Operation tags
  // ---------------------------------------------------------------------------

  // Tags are per thread (inherited by child threads as a copy) and attached to every execution.
  private[this] val tags = new InheritableThreadLocal[mutable.Set[String]] {
    override def childValue(parent: mutable.Set[String]): mutable.Set[String] = parent.clone()
    override protected def initialValue(): mutable.Set[String] = new mutable.HashSet[String]()
  }

  private[sql] def addTag(tag: String): Unit = {
    throwIfInvalidTag(tag)
    tags.get += tag
  }

  private[sql] def removeTag(tag: String): Unit = {
    throwIfInvalidTag(tag)
    tags.get -= tag
  }

  private[sql] def getTags(): Set[String] = tags.get.toSet

  private[sql] def clearTags(): Unit = tags.get.clear()

  // ---------------------------------------------------------------------------
  // ExecutePlan
  // ---------------------------------------------------------------------------

  /**
   * Executes `plan`, always sending an operation id (a fresh UUID unless one is given) so the
   * operation can be interrupted or looked up with [[getOperationStatuses]], and attaching the
   * thread's operation tags.
   */
  private[sql] def execute(
      plan: proto.Plan,
      operationId: Option[String] = None
  ): Iterator[proto.ExecutePlanResponse] = {
    val resolvedOperationId = operationId.getOrElse(UUID.randomUUID().toString)
    require(
      isValidUUID(resolvedOperationId),
      s"Invalid operationId: $resolvedOperationId. The id must be an UUID string of " +
        "the format `00112233-4455-6677-8899-aabbccddeeff`"
    )
    val request = proto.ExecutePlanRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      plan = Some(plan),
      clientType = Some(userAgent),
      operationId = Some(resolvedOperationId),
      tags = tags.get.toSeq
    )
    val responses =
      if (configuration.useReattachableExecute) {
        // Resilient: resumes a broken stream via ReattachExecute and retries transient errors.
        new ExecutePlanResponseReattachableIterator(request, reattachableStub, retryHandler)
      } else {
        // Non-reattachable: retry only the initial call (safe before any response is consumed).
        // No deadline here: timing out would kill the execution with no way to resume it.
        retryHandler.retry(stub.executePlan(request))
      }
    exceptions.convertIterator(responses, Some(resolvedOperationId))
  }

  // ---------------------------------------------------------------------------
  // AnalyzePlan
  // ---------------------------------------------------------------------------

  private[sql] def analyze(analyze: proto.AnalyzePlanRequest.Analyze): proto.AnalyzePlanResponse = {
    val request = proto.AnalyzePlanRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      clientType = Some(userAgent),
      analyze = analyze
    )
    exceptions.convert(
      retryHandler.retry(
        stubWithDeadline(configuration.rpcDeadlines.analyzePlan).analyzePlan(request)
      )
    )
  }

  private[sql] def analyzeSchema(plan: proto.Plan): proto.DataType =
    analyze(
      proto.AnalyzePlanRequest.Analyze
        .Schema(proto.AnalyzePlanRequest.Schema(plan = Some(plan)))
    ).getSchema.getSchema

  private[sql] def explainString(
      plan: proto.Plan,
      mode: proto.AnalyzePlanRequest.Explain.ExplainMode
  ): String =
    analyze(
      proto.AnalyzePlanRequest.Analyze.Explain(
        proto.AnalyzePlanRequest.Explain(plan = Some(plan), explainMode = mode)
      )
    ).getExplain.explainString

  private[sql] def treeString(plan: proto.Plan, level: Option[Int]): String =
    analyze(
      proto.AnalyzePlanRequest.Analyze.TreeString(
        proto.AnalyzePlanRequest.TreeString(plan = Some(plan), level = level)
      )
    ).getTreeString.treeString

  private[sql] def isLocal(plan: proto.Plan): Boolean =
    analyze(
      proto.AnalyzePlanRequest.Analyze
        .IsLocal(proto.AnalyzePlanRequest.IsLocal(plan = Some(plan)))
    ).getIsLocal.isLocal

  private[sql] def isStreaming(plan: proto.Plan): Boolean =
    analyze(
      proto.AnalyzePlanRequest.Analyze.IsStreaming(
        proto.AnalyzePlanRequest.IsStreaming(plan = Some(plan))
      )
    ).getIsStreaming.isStreaming

  private[sql] def inputFiles(plan: proto.Plan): Seq[String] =
    analyze(
      proto.AnalyzePlanRequest.Analyze.InputFiles(
        proto.AnalyzePlanRequest.InputFiles(plan = Some(plan))
      )
    ).getInputFiles.files

  private[sql] def sparkVersion: String =
    analyze(
      proto.AnalyzePlanRequest.Analyze
        .SparkVersion(proto.AnalyzePlanRequest.SparkVersion())
    ).getSparkVersion.version

  private[sql] def semanticHash(plan: proto.Plan): Int =
    analyze(
      proto.AnalyzePlanRequest.Analyze.SemanticHash(
        proto.AnalyzePlanRequest.SemanticHash(plan = Some(plan))
      )
    ).getSemanticHash.result

  private[sql] def sameSemantics(plan: proto.Plan, otherPlan: proto.Plan): Boolean =
    analyze(
      proto.AnalyzePlanRequest.Analyze.SameSemantics(
        proto.AnalyzePlanRequest.SameSemantics(targetPlan = Some(plan), otherPlan = Some(otherPlan))
      )
    ).getSameSemantics.result

  /**
   * Parses a DDL-formatted schema or data type string (e.g. `"a INT, b STRING"`) on the server, for
   * the places where the protocol needs a resolved type rather than a string.
   */
  private[sql] def ddlParse(ddl: String): proto.DataType =
    analyze(
      proto.AnalyzePlanRequest.Analyze.DdlParse(proto.AnalyzePlanRequest.DDLParse(ddlString = ddl))
    ).getDdlParse.getParsed

  // ---------------------------------------------------------------------------
  // Config
  // ---------------------------------------------------------------------------

  private def config(operation: proto.ConfigRequest.Operation): proto.ConfigResponse = {
    val request = proto.ConfigRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      clientType = Some(userAgent),
      operation = Some(operation)
    )
    exceptions.convert(
      retryHandler.retry(stubWithDeadline(configuration.rpcDeadlines.config).config(request))
    )
  }

  private[sql] def setConf(key: String, value: String): Unit =
    config(
      proto.ConfigRequest.Operation(
        proto.ConfigRequest.Operation.OpType
          .Set(proto.ConfigRequest.Set(pairs = Seq(proto.KeyValue(key = key, value = Some(value)))))
      )
    )

  private[sql] def getConf(key: String): String =
    getConfOption(key).getOrElse(throw new NoSuchElementException(s"Config key not found: $key"))

  private[sql] def getConfOption(key: String): Option[String] = {
    val response = config(
      proto.ConfigRequest.Operation(
        proto.ConfigRequest.Operation.OpType
          .GetOption(proto.ConfigRequest.GetOption(keys = Seq(key)))
      )
    )
    response.pairs.headOption.flatMap(_.value)
  }

  private[sql] def getConfWithDefault(key: String, default: String): String = {
    val response = config(
      proto.ConfigRequest.Operation(
        proto.ConfigRequest.Operation.OpType.GetWithDefault(
          proto.ConfigRequest.GetWithDefault(
            pairs = Seq(proto.KeyValue(key = key, value = Some(default)))
          )
        )
      )
    )
    response.pairs.headOption.flatMap(_.value).getOrElse(default)
  }

  private[sql] def getAllConfs: Map[String, String] = {
    val response = config(
      proto.ConfigRequest.Operation(
        proto.ConfigRequest.Operation.OpType.GetAll(proto.ConfigRequest.GetAll())
      )
    )
    response.pairs.map(kv => kv.key -> kv.value.getOrElse("")).toMap
  }

  private[sql] def unsetConf(key: String): Unit =
    config(
      proto.ConfigRequest.Operation(
        proto.ConfigRequest.Operation.OpType.Unset(proto.ConfigRequest.Unset(keys = Seq(key)))
      )
    )

  private[sql] def isModifiable(key: String): Boolean = {
    val response = config(
      proto.ConfigRequest.Operation(
        proto.ConfigRequest.Operation.OpType
          .IsModifiable(proto.ConfigRequest.IsModifiable(keys = Seq(key)))
      )
    )
    response.pairs.headOption.flatMap(_.value).exists(_.toBoolean)
  }

  // ---------------------------------------------------------------------------
  // Interrupt, status & lifecycle
  // ---------------------------------------------------------------------------

  private def interrupt(
      interruptType: proto.InterruptRequest.InterruptType,
      interrupt: proto.InterruptRequest.Interrupt = proto.InterruptRequest.Interrupt.Empty
  ): Seq[String] = {
    val response = exceptions.convert(
      stubWithDeadline(configuration.rpcDeadlines.interrupt).interrupt(
        proto.InterruptRequest(
          sessionId = sessionId,
          userContext = Some(userContext),
          clientType = Some(userAgent),
          interruptType = interruptType,
          interrupt = interrupt
        )
      )
    )
    response.interruptedIds
  }

  private[sql] def interruptAll(): Seq[String] =
    interrupt(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)

  private[sql] def interruptTag(tag: String): Seq[String] =
    interrupt(
      proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG,
      proto.InterruptRequest.Interrupt.OperationTag(tag)
    )

  private[sql] def interruptOperation(operationId: String): Seq[String] =
    interrupt(
      proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID,
      proto.InterruptRequest.Interrupt.OperationId(operationId)
    )

  /**
   * Returns the status of operations in this session (Spark 4.2+ servers): of the operations with
   * the given ids, or of all operations when `operationIds` is empty.
   *
   * @param operationIds
   *   operation ids to report on; empty means all operations in the session.
   * @param operationExtensions
   *   per-operation extension messages to include in the request.
   * @param requestExtensions
   *   request-level extension messages to include in the request.
   * @return
   *   the full `GetStatusResponse`, including `operationStatuses` and any extensions.
   */
  def getOperationStatuses(
      operationIds: Seq[String] = Seq.empty,
      operationExtensions: Seq[com.google.protobuf.any.Any] = Seq.empty,
      requestExtensions: Seq[com.google.protobuf.any.Any] = Seq.empty
  ): proto.GetStatusResponse = {
    val request = proto.GetStatusRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      clientType = Some(userAgent),
      operationStatus = Some(
        proto.GetStatusRequest.OperationStatusRequest(
          operationIds = operationIds,
          extensions = operationExtensions
        )
      ),
      extensions = requestExtensions
    )
    exceptions.convert(
      retryHandler.retry(stubWithDeadline(configuration.rpcDeadlines.getStatus).getStatus(request))
    )
  }

  /** Releases the server-side session (its state, caches, and temporary views). */
  private[sql] def releaseSession(): Unit =
    exceptions.convert(
      stubWithDeadline(configuration.rpcDeadlines.releaseSession).releaseSession(
        proto.ReleaseSessionRequest(
          sessionId = sessionId,
          userContext = Some(userContext),
          clientType = Some(userAgent)
        )
      )
    )

  /** Returns a new client with its own channel and a fresh session id. */
  private[sql] def copy(): SparkConnectClient =
    configuration.copy(sessionId = None).toSparkConnectClient

  /**
   * Clones this session on the server (Spark 4.1+), copying its SQL configurations, temporary
   * views, registered functions, and catalog state into a new independent session, and returns a
   * client bound to the clone. `newSessionId` must be a UUID when given.
   */
  private[sql] def cloneSession(newSessionId: Option[String] = None): SparkConnectClient = {
    newSessionId.foreach { id =>
      require(isValidUUID(id), s"Invalid session id: $id. The id must be an UUID string.")
    }
    val response = exceptions.convert(
      stubWithDeadline(configuration.rpcDeadlines.cloneSession).cloneSession(
        proto.CloneSessionRequest(
          sessionId = sessionId,
          userContext = Some(userContext),
          clientType = Some(userAgent),
          newSessionId = newSessionId
        )
      )
    )
    newSessionId.foreach { expected =>
      require(
        response.newSessionId == expected,
        s"Returned session ID '${response.newSessionId}' does not match requested ID '$expected'"
      )
    }
    configuration.copy(sessionId = Some(response.newSessionId)).toSparkConnectClient
  }

  def shutdown(): Unit = {
    // Try a graceful shutdown first so in-flight RPCs can finish and the server can release
    // their execution state; fall back to a forced shutdown if they do not drain in time.
    channel.shutdown()
    if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
      channel.shutdownNow()
      channel.awaitTermination(5, TimeUnit.SECONDS)
    }
  }
}

object SparkConnectClient {

  def builder(): Builder = new Builder()

  // Same rules as Spark job tags: the Connect tag is part of the server-side job tag.
  private def throwIfInvalidTag(tag: String): Unit = {
    if (tag == null) {
      throw new IllegalArgumentException("Spark Connect tag cannot be null.")
    }
    if (tag.contains(",")) {
      throw new IllegalArgumentException("Spark Connect tag cannot contain ','.")
    }
    if (tag.isEmpty) {
      throw new IllegalArgumentException("Spark Connect tag cannot be an empty string.")
    }
  }

  private def isValidUUID(uuid: String): Boolean =
    try { UUID.fromString(uuid); true }
    catch { case _: IllegalArgumentException => false }

  /** Carries the operation id of a failed execution on the converted exception. */
  private final class SparkConnectOperationIdException(val operationId: String)
      extends RuntimeException(s"Spark Connect operation ID: $operationId", null, false, false)

  /**
   * Returns the Spark Connect operation id of the execution that raised `error`, if the error was
   * raised by an `ExecutePlan` call of this client (e.g. to pass to `interruptOperation` or
   * [[SparkConnectClient.getOperationStatuses]]).
   */
  def getOperationId(error: Throwable): Option[String] =
    error.getSuppressed.collectFirst { case marker: SparkConnectOperationIdException =>
      marker.operationId
    }

  private[client] def attachOperationId(error: Throwable, operationId: String): Unit =
    if (getOperationId(error).isEmpty) {
      error.addSuppressed(new SparkConnectOperationIdException(operationId))
    }

  /**
   * Immutable connection configuration. Construct via [[Builder]] or [[Builder.connectionString]].
   *
   * The keepalive settings make the client send HTTP/2 PINGs so a connection that silently died
   * (e.g. a NAT gateway dropped it) fails with `UNAVAILABLE` instead of hanging; they default to
   * Apache Spark 4.3's values (enabled, 60s, 20s, also without calls).
   */
  case class Configuration(
      host: String = "localhost",
      port: Int = Configuration.DEFAULT_PORT,
      useSsl: Boolean = false,
      token: Option[String] = None,
      userId: Option[String] = Option(System.getProperty("user.name")),
      userName: Option[String] = None,
      userAgent: String = Configuration.DEFAULT_USER_AGENT,
      sessionId: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
      maxInboundMessageSize: Int = Configuration.MAX_MESSAGE_SIZE,
      useReattachableExecute: Boolean = true,
      retryPolicies: Seq[RetryPolicy] = RetryPolicy.defaultPolicies(),
      rpcDeadlines: RpcDeadlines = RpcDeadlines(),
      maxRetryExceptionElapsedTime: FiniteDuration =
        GrpcRetryHandler.DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
      grpcKeepAliveEnabled: Boolean = Configuration.DEFAULT_GRPC_KEEPALIVE_ENABLED,
      grpcKeepAliveTimeMs: Long = Configuration.DEFAULT_GRPC_KEEPALIVE_TIME_MS,
      grpcKeepAliveTimeoutMs: Long = Configuration.DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
      grpcKeepAliveWithoutCalls: Boolean = Configuration.DEFAULT_GRPC_KEEPALIVE_WITHOUT_CALLS
  ) {

    def toChannel: ManagedChannel = {
      val builder = ManagedChannelBuilder.forAddress(host, port)
      if (useSsl || token.isDefined) builder.useTransportSecurity() else builder.usePlaintext()
      builder.maxInboundMessageSize(maxInboundMessageSize)
      if (grpcKeepAliveEnabled) {
        builder.keepAliveTime(grpcKeepAliveTimeMs, TimeUnit.MILLISECONDS)
        builder.keepAliveTimeout(grpcKeepAliveTimeoutMs, TimeUnit.MILLISECONDS)
        builder.keepAliveWithoutCalls(grpcKeepAliveWithoutCalls)
      }

      val headers = new GrpcMetadata()
      token.foreach { t =>
        headers.put(
          GrpcMetadata.Key.of("authorization", GrpcMetadata.ASCII_STRING_MARSHALLER),
          s"Bearer $t"
        )
      }
      metadata.foreach { case (k, v) =>
        if (k.endsWith(GrpcMetadata.BINARY_HEADER_SUFFIX)) {
          // Binary ("-bin") headers carry base64-encoded bytes, as in Apache Spark.
          headers.put(
            GrpcMetadata.Key.of(k, GrpcMetadata.BINARY_BYTE_MARSHALLER),
            Base64.getDecoder.decode(v.getBytes(UTF_8))
          )
        } else {
          headers.put(GrpcMetadata.Key.of(k, GrpcMetadata.ASCII_STRING_MARSHALLER), v)
        }
      }
      if (!headers.keys().isEmpty) {
        builder.intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
      }
      builder.build()
    }

    def toSparkConnectClient: SparkConnectClient = new SparkConnectClient(this, toChannel)
  }

  object Configuration {
    val DEFAULT_PORT = 15002
    val MAX_MESSAGE_SIZE: Int = 128 * 1024 * 1024
    val DEFAULT_USER_AGENT: String =
      sys.env.getOrElse("SPARK_CONNECT_USER_AGENT", "spark-connect-scala3")
    val DEFAULT_GRPC_KEEPALIVE_ENABLED: Boolean = true
    val DEFAULT_GRPC_KEEPALIVE_TIME_MS: Long = 60 * 1000L
    val DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS: Long = 20 * 1000L
    val DEFAULT_GRPC_KEEPALIVE_WITHOUT_CALLS: Boolean = true
  }

  /** Fluent builder for a [[SparkConnectClient]]. */
  class Builder(private var configuration: Configuration = Configuration()) {

    def configuration(c: Configuration): Builder = { configuration = c; this }

    def host(inputHost: String): Builder = {
      configuration = configuration.copy(host = inputHost)
      this
    }

    def port(inputPort: Int): Builder = {
      configuration = configuration.copy(port = inputPort)
      this
    }

    def userId(id: String): Builder = {
      configuration = configuration.copy(userId = Option(id))
      this
    }

    def userName(name: String): Builder = {
      configuration = configuration.copy(userName = Option(name))
      this
    }

    def userAgent(agent: String): Builder = {
      configuration = configuration.copy(userAgent = agent)
      this
    }

    def sessionId(id: String): Builder = {
      configuration = configuration.copy(sessionId = Option(id))
      this
    }

    def token(t: String): Builder = {
      configuration = configuration.copy(token = Option(t), useSsl = true)
      this
    }

    def enableSsl(): Builder = {
      configuration = configuration.copy(useSsl = true)
      this
    }

    /** Enables or disables reattachable execution (resilient, resumable result streams). */
    def reattachable(enabled: Boolean): Builder = {
      configuration = configuration.copy(useReattachableExecute = enabled)
      this
    }

    /** Sets the per-RPC deadlines; use [[RpcDeadlines.disabled]] to turn them all off. */
    def rpcDeadlines(deadlines: RpcDeadlines): Builder = {
      configuration = configuration.copy(rpcDeadlines = deadlines)
      this
    }

    /** Bounds the total time spent resuming an execution that keeps needing to reattach. */
    def maxRetryExceptionElapsedTime(duration: FiniteDuration): Builder = {
      configuration = configuration.copy(maxRetryExceptionElapsedTime = duration)
      this
    }

    /** Whether to send gRPC/HTTP2 keepalive PINGs to detect a silently-dead connection. */
    def grpcKeepAliveEnabled(enabled: Boolean): Builder = {
      configuration = configuration.copy(grpcKeepAliveEnabled = enabled)
      this
    }

    def grpcKeepAliveEnabled: Boolean = configuration.grpcKeepAliveEnabled

    /** Idle time before sending a keepalive PING, in milliseconds. */
    def grpcKeepAliveTimeMs(timeMs: Long): Builder = {
      configuration = configuration.copy(grpcKeepAliveTimeMs = timeMs)
      this
    }

    def grpcKeepAliveTimeMs: Long = configuration.grpcKeepAliveTimeMs

    /** Time to wait for a keepalive PING acknowledgement before failing, in milliseconds. */
    def grpcKeepAliveTimeoutMs(timeoutMs: Long): Builder = {
      configuration = configuration.copy(grpcKeepAliveTimeoutMs = timeoutMs)
      this
    }

    def grpcKeepAliveTimeoutMs: Long = configuration.grpcKeepAliveTimeoutMs

    /** Whether to keep sending keepalive PINGs while there are no in-flight RPCs. */
    def grpcKeepAliveWithoutCalls(enabled: Boolean): Builder = {
      configuration = configuration.copy(grpcKeepAliveWithoutCalls = enabled)
      this
    }

    def grpcKeepAliveWithoutCalls: Boolean = configuration.grpcKeepAliveWithoutCalls

    def option(key: String, value: String): Builder = {
      configuration = configuration.copy(metadata = configuration.metadata + (key -> value))
      this
    }

    /**
     * Configures the builder from a Spark Connect connection string of the form
     * `sc://host:port/;param1=value1;param2=value2`.
     */
    def connectionString(connectionString: String): Builder = {
      configuration = parseConnectionString(connectionString, configuration)
      this
    }

    def build(): SparkConnectClient = configuration.toSparkConnectClient
  }

  private val SC_PREFIX = "sc://"

  private[client] def parseConnectionString(
      connectionString: String,
      base: Configuration
  ): Configuration = {
    require(
      connectionString.startsWith(SC_PREFIX),
      s"Connection string must start with '$SC_PREFIX', but was: $connectionString"
    )

    val withoutPrefix = connectionString.stripPrefix(SC_PREFIX)
    val slashIdx = withoutPrefix.indexOf('/')
    val (endpoint, paramsPart) =
      if (slashIdx < 0) (withoutPrefix, "")
      else (withoutPrefix.substring(0, slashIdx), withoutPrefix.substring(slashIdx + 1))

    var config = base
    if (endpoint.nonEmpty) {
      val colonIdx = endpoint.lastIndexOf(':')
      if (colonIdx < 0) {
        config = config.copy(host = endpoint)
      } else {
        val host = endpoint.substring(0, colonIdx)
        val portStr = endpoint.substring(colonIdx + 1)
        val port = portStr.toIntOption.getOrElse(
          throw new IllegalArgumentException(s"Invalid port in connection string: $portStr")
        )
        config = config.copy(host = host, port = port)
      }
    }

    val params = paramsPart
      .split(';')
      .iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { token =>
        val eq = token.indexOf('=')
        require(eq > 0, s"Malformed connection string parameter: '$token'")
        token.substring(0, eq) -> token.substring(eq + 1)
      }

    params.foreach {
      case ("user_id", v) => config = config.copy(userId = Option(v))
      case ("user_name", v) => config = config.copy(userName = Option(v))
      case ("user_agent", v) => config = config.copy(userAgent = v)
      case ("token", v) => config = config.copy(token = Option(v), useSsl = true)
      case ("use_ssl", v) => config = config.copy(useSsl = v.toBoolean)
      case ("session_id", v) => config = config.copy(sessionId = Option(v))
      case ("grpc_max_message_size", v) =>
        config = config.copy(maxInboundMessageSize = v.toInt)
      case ("grpc_keepalive_enabled", v) => config = config.copy(grpcKeepAliveEnabled = v.toBoolean)
      case ("grpc_keepalive_time_ms", v) => config = config.copy(grpcKeepAliveTimeMs = v.toLong)
      case ("grpc_keepalive_timeout_ms", v) =>
        config = config.copy(grpcKeepAliveTimeoutMs = v.toLong)
      case ("grpc_keepalive_without_calls", v) =>
        config = config.copy(grpcKeepAliveWithoutCalls = v.toBoolean)
      case (k, v) => config = config.copy(metadata = config.metadata + (k -> v))
    }
    config
  }
}
