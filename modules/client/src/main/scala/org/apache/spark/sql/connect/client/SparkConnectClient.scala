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

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata => GrpcMetadata}
import io.grpc.stub.MetadataUtils

import org.apache.spark.connect.proto

/**
 * The low-level client that talks to a Spark Connect server over gRPC.
 *
 * It owns the gRPC channel, the (stable) session id, the user context and the user agent, and
 * exposes the four core RPCs used by the public API: `ExecutePlan`, `AnalyzePlan`, `Config` and
 * `Interrupt`.
 */
class SparkConnectClient private[sql] (
    private[sql] val configuration: SparkConnectClient.Configuration,
    private[sql] val channel: ManagedChannel
) {

  import SparkConnectClient._

  private val stub = proto.SparkConnectServiceGrpc.blockingStub(channel)
  private val retryHandler = new GrpcRetryHandler(configuration.retryPolicies)
  private lazy val reattachableStub = new GrpcReattachableStub(channel)

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

  // ---------------------------------------------------------------------------
  // ExecutePlan
  // ---------------------------------------------------------------------------

  private[sql] def execute(plan: proto.Plan): Iterator[proto.ExecutePlanResponse] = {
    val request = proto.ExecutePlanRequest(
      sessionId = sessionId,
      userContext = Some(userContext),
      plan = Some(plan),
      clientType = Some(userAgent)
    )
    val responses =
      if (configuration.useReattachableExecute) {
        // Resilient: resumes a broken stream via ReattachExecute and retries transient errors.
        new ExecutePlanResponseReattachableIterator(request, reattachableStub, retryHandler)
      } else {
        // Non-reattachable: retry only the initial call (safe before any response is consumed).
        retryHandler.retry(stub.executePlan(request))
      }
    GrpcExceptionConverter.convertIterator(responses)
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
    GrpcExceptionConverter.convert(retryHandler.retry(stub.analyzePlan(request)))
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
    GrpcExceptionConverter.convert(retryHandler.retry(stub.config(request)))
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

  // ---------------------------------------------------------------------------
  // Interrupt & lifecycle
  // ---------------------------------------------------------------------------

  private[sql] def interruptAll(): Seq[String] = {
    val response = GrpcExceptionConverter.convert(
      stub.interrupt(
        proto.InterruptRequest(
          sessionId = sessionId,
          userContext = Some(userContext),
          clientType = Some(userAgent),
          interruptType = proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL
        )
      )
    )
    response.interruptedIds
  }

  private[sql] def interruptTag(tag: String): Seq[String] = {
    val response = GrpcExceptionConverter.convert(
      stub.interrupt(
        proto.InterruptRequest(
          sessionId = sessionId,
          userContext = Some(userContext),
          clientType = Some(userAgent),
          interruptType = proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG,
          interrupt = proto.InterruptRequest.Interrupt.OperationTag(tag)
        )
      )
    )
    response.interruptedIds
  }

  /** Returns a new client that shares the channel but starts a fresh session id. */
  private[sql] def copy(): SparkConnectClient =
    new SparkConnectClient(configuration.copy(sessionId = None), channel)

  def shutdown(): Unit = {
    channel.shutdownNow()
    channel.awaitTermination(10, TimeUnit.SECONDS)
  }
}

object SparkConnectClient {

  def builder(): Builder = new Builder()

  /**
   * Immutable connection configuration. Construct via [[Builder]] or [[Builder.connectionString]].
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
      retryPolicies: Seq[RetryPolicy] = RetryPolicy.defaultPolicies()
  ) {

    def toChannel: ManagedChannel = {
      val builder = ManagedChannelBuilder.forAddress(host, port)
      if (useSsl || token.isDefined) builder.useTransportSecurity() else builder.usePlaintext()
      builder.maxInboundMessageSize(maxInboundMessageSize)

      val headers = new GrpcMetadata()
      token.foreach { t =>
        headers.put(
          GrpcMetadata.Key.of("authorization", GrpcMetadata.ASCII_STRING_MARSHALLER),
          s"Bearer $t"
        )
      }
      metadata.foreach { case (k, v) =>
        headers.put(GrpcMetadata.Key.of(k, GrpcMetadata.ASCII_STRING_MARSHALLER), v)
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
      case (k, v) => config = config.copy(metadata = config.metadata + (k -> v))
    }
    config
  }
}
