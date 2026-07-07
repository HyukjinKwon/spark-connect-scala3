# Plugin extensions

Spark Connect is extensible: a server can register plugins that handle custom
relations, expressions, and commands. A [`RelationPlugin`][relplugin] (and its
`ExpressionPlugin` / `CommandPlugin` siblings) receives an arbitrary protobuf
message -- shipped in the `extension` field of the plan -- and turns it into a
Spark logical plan on the server. Projects like [GraphFrames][graphframes] use
this to expose graph algorithms (Pregel, connected components, ...) over Connect.

This client lets you build such an `extension` relation and turn it into a
`DataFrame`, mirroring the PySpark client's
`plan.extension.Pack(msg)` / `DataFrame(plan, session)` flow.

## The entry points

These methods are public for plugin authors (they mirror what upstream Apache
Spark's Connect client marks `@DeveloperApi`):

| Method | Purpose |
| ------ | ------- |
| `SparkSession.newDataFrame(extension: com.google.protobuf.any.Any)` | Wrap an already packed message in an `extension` relation and return a `DataFrame`. This is the one you usually want. |
| `SparkSession.newDataFrame(relType: proto.Relation.RelType)` | Build a `DataFrame` from any relation you construct yourself. |
| `SparkSession.newRelation(relType: proto.Relation.RelType)` | Wrap a `RelType` in a `Relation` tagged with a unique plan id, without building a `DataFrame`. |
| `Dataset.relation` | The protobuf `Relation` backing an existing `DataFrame`, so you can embed it as an input inside your plugin message. |
| `Dataset.plan` | The full `Plan` (a root plan wrapping the relation) that would be sent to the server. |

The protobuf types live in the `org.apache.spark.connect.proto` package, and the
`google.protobuf.Any` wrapper (with `pack` / `unpack`) comes from ScalaPB's
`com.google.protobuf.any.Any`. Both are on the classpath transitively via the
client artifact.

## Building an extension DataFrame

A plugin defines its own `.proto` and generates message classes from it (with
ScalaPB, or by reusing the plugin's published generated code). Pack an instance
into a `google.protobuf.Any` and hand it to `newDataFrame`:

```scala
import com.google.protobuf.any.{Any => ProtoAny}
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

// `MyPluginMessage` is generated from the plugin's own .proto.
val message = MyPluginMessage(/* ... */)
val df = spark.newDataFrame(ProtoAny.pack(message))

df.show()
```

### Embedding existing DataFrames

A plugin message often needs to reference upstream DataFrames as inputs -- for
example, GraphFrames' `GraphFramesAPI` message carries `vertices` and `edges`
relations. Use `Dataset.relation` to pull the protobuf plan out of an existing
`DataFrame`, the counterpart of PySpark's `dataframe_to_proto(df, session)`:

```scala
import com.google.protobuf.any.{Any => ProtoAny}

val vertices = spark.read.parquet("/data/vertices")
val edges = spark.read.parquet("/data/edges")

// GraphFramesAPI / Pregel are ScalaPB messages generated from the plugin's .proto.
val message = GraphFramesAPI(
  vertices = Some(vertices.relation),
  edges    = Some(edges.relation)
).withPregel(Pregel(/* ... */))

val result: DataFrame = spark.newDataFrame(ProtoAny.pack(message))
```

## Server-side registration

The client only *builds and sends* the plan. Execution requires the matching
plugin to be registered on the **server**, via the
`spark.connect.extensions.relation.classes` configuration (and the analogous
`expression.classes` / `command.classes` for the other plugin kinds):

```bash
./sbin/start-connect-server.sh \
  --jars /path/to/your-plugin.jar \
  --conf spark.connect.extensions.relation.classes=com.example.MyRelationPlugin
```

The server routes *every* `extension` relation to a registered plugin; it does
not interpret the packed message itself. If no plugin handles the message type,
the server responds with `No handler found for extension`. That is the exact code
path a real plugin hooks into -- so seeing it against a stock server confirms the
client built and delivered the plan correctly.

## Runnable example

See [`RelationExtension`][example] in `modules/examples/`. It packs a message,
builds the `extension` DataFrame, prints the resulting plan (relation type and
the packed message's type URL), and attempts execution -- reporting the
"no handler" case cleanly when run against a server without the plugin installed.

```bash
sbt "examples/runMain examples.RelationExtension sc://localhost:15002"
```

[relplugin]: https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/connect/plugin/RelationPlugin.html
[graphframes]: https://graphframes.io/
[example]: https://github.com/HyukjinKwon/spark-connect-scala3/blob/main/modules/examples/src/main/scala/examples/RelationExtension.scala
