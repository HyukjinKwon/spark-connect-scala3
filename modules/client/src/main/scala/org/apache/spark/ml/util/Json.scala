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
package org.apache.spark.ml.util

/**
 * A minimal JSON reader and writer for ML metadata files. Numbers keep their source text so that
 * integers and doubles round-trip exactly as Spark writes them.
 */
private[ml] object Json {

  sealed trait Value
  case object Null extends Value
  final case class Bool(value: Boolean) extends Value
  final case class Num(text: String) extends Value
  final case class Str(value: String) extends Value
  final case class Arr(values: Seq[Value]) extends Value
  final case class Obj(fields: Seq[(String, Value)]) extends Value {
    def get(key: String): Option[Value] = fields.collectFirst { case (k, v) if k == key => v }
    def getStr(key: String): String = get(key) match {
      case Some(Str(s)) => s
      case other => throw new IllegalArgumentException(s"Expected string for $key, got $other")
    }
  }

  /** The JSON text of a value. */
  def render(value: Value): String = value match {
    case Null => "null"
    case Bool(b) => b.toString
    case Num(t) => t
    case Str(s) => quote(s)
    case Arr(vs) => vs.map(render).mkString("[", ",", "]")
    case Obj(fs) => fs.map { case (k, v) => quote(k) + ":" + render(v) }.mkString("{", ",", "}")
  }

  private def quote(s: String): String = {
    val sb = new StringBuilder("\"")
    s.foreach {
      case '"' => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < ' ' => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.append('"').toString
  }

  /** Parses a JSON document. */
  def parse(text: String): Value = {
    val p = new Parser(text)
    val v = p.value()
    p.skipWs()
    require(p.atEnd, s"Trailing characters in JSON at ${p.pos}")
    v
  }

  private final class Parser(text: String) {
    var pos = 0
    def atEnd: Boolean = pos >= text.length

    def skipWs(): Unit = while (pos < text.length && text(pos).isWhitespace) pos += 1

    def value(): Value = {
      skipWs()
      require(!atEnd, "Unexpected end of JSON")
      text(pos) match {
        case '{' => obj()
        case '[' => arr()
        case '"' => Str(str())
        case 't' => lit("true", Bool(true))
        case 'f' => lit("false", Bool(false))
        case 'n' => lit("null", Null)
        case _ => num()
      }
    }

    private def lit(word: String, v: Value): Value = {
      require(text.startsWith(word, pos), s"Bad JSON literal at $pos")
      pos += word.length
      v
    }

    private def num(): Value = {
      val start = pos
      while (pos < text.length && "+-0123456789.eE".indexOf(text(pos)) >= 0) pos += 1
      require(pos > start, s"Bad JSON number at $pos")
      Num(text.substring(start, pos))
    }

    private def str(): String = {
      pos += 1 // opening quote
      val sb = new StringBuilder
      while (text(pos) != '"')
        if (text(pos) == '\\') {
          text(pos + 1) match {
            case 'n' => sb.append('\n')
            case 'r' => sb.append('\r')
            case 't' => sb.append('\t')
            case 'b' => sb.append('\b')
            case 'f' => sb.append('\f')
            case 'u' =>
              sb.append(Integer.parseInt(text.substring(pos + 2, pos + 6), 16).toChar)
              pos += 4
            case c => sb.append(c)
          }
          pos += 2
        } else {
          sb.append(text(pos))
          pos += 1
        }
      pos += 1 // closing quote
      sb.toString
    }

    private def arr(): Value = {
      pos += 1
      val items = Seq.newBuilder[Value]
      skipWs()
      if (text(pos) == ']') { pos += 1; return Arr(Nil) }
      var more = true
      while (more) {
        items += value()
        skipWs()
        if (text(pos) == ',') pos += 1 else { require(text(pos) == ']'); more = false }
      }
      pos += 1
      Arr(items.result())
    }

    private def obj(): Value = {
      pos += 1
      val fields = Seq.newBuilder[(String, Value)]
      skipWs()
      if (text(pos) == '}') { pos += 1; return Obj(Nil) }
      var more = true
      while (more) {
        skipWs()
        val k = str()
        skipWs()
        require(text(pos) == ':', s"Expected ':' at $pos")
        pos += 1
        fields += (k -> value())
        skipWs()
        if (text(pos) == ',') pos += 1 else { require(text(pos) == '}'); more = false }
      }
      pos += 1
      Obj(fields.result())
    }
  }
}
