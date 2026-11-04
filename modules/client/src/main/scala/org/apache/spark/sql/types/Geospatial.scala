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

package org.apache.spark.sql.types

/**
 * A GEOMETRY value: its Well-Known Binary (WKB) bytes plus a spatial reference identifier. Returned
 * for [[GeometryType]] columns. Mirrors `org.apache.spark.sql.types.Geometry`.
 */
final class Geometry private (value: Array[Byte], srid: Int) extends Serializable {

  /** The WKB representation of this geometry. */
  def getBytes: Array[Byte] = value

  /** The spatial reference identifier of this geometry. */
  def getSrid: Int = srid

  override def equals(obj: Any): Boolean = obj match {
    case other: Geometry => srid == other.getSrid && java.util.Arrays.equals(value, other.getBytes)
    case _ => false
  }

  override def hashCode(): Int = 31 * java.util.Arrays.hashCode(value) + Integer.hashCode(srid)

  override def toString: String = s"Geometry(srid=$srid, wkb=${value.length} bytes)"
}

object Geometry {

  /** Default SRID of a [[Geometry]] created without one. */
  val DEFAULT_SRID: Int = 0

  /** Creates a [[Geometry]] from WKB bytes and an SRID. */
  def fromWKB(bytes: Array[Byte], srid: Int): Geometry = new Geometry(bytes, srid)

  /** Creates a [[Geometry]] from WKB bytes with the default SRID. */
  def fromWKB(bytes: Array[Byte]): Geometry = new Geometry(bytes, DEFAULT_SRID)
}

/**
 * A GEOGRAPHY value: its Well-Known Binary (WKB) bytes plus a spatial reference identifier.
 * Returned for [[GeographyType]] columns. Mirrors `org.apache.spark.sql.types.Geography`.
 */
final class Geography private (value: Array[Byte], srid: Int) extends Serializable {

  /** The WKB representation of this geography. */
  def getBytes: Array[Byte] = value

  /** The spatial reference identifier of this geography. */
  def getSrid: Int = srid

  override def equals(obj: Any): Boolean = obj match {
    case other: Geography =>
      srid == other.getSrid && java.util.Arrays.equals(value, other.getBytes)
    case _ => false
  }

  override def hashCode(): Int = 31 * java.util.Arrays.hashCode(value) + Integer.hashCode(srid)

  override def toString: String = s"Geography(srid=$srid, wkb=${value.length} bytes)"
}

object Geography {

  /** Default SRID of a [[Geography]] created without one (WGS 84). */
  val DEFAULT_SRID: Int = 4326

  /** Creates a [[Geography]] from WKB bytes and an SRID. */
  def fromWKB(bytes: Array[Byte], srid: Int): Geography = new Geography(bytes, srid)

  /** Creates a [[Geography]] from WKB bytes with the default SRID. */
  def fromWKB(bytes: Array[Byte]): Geography = new Geography(bytes, DEFAULT_SRID)
}
