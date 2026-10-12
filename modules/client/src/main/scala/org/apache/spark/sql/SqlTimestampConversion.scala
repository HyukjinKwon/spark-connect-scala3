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

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime}
import java.time.temporal.ChronoField
import java.util.{Calendar, TimeZone}

/** Spark's SQL dates and timestamps use hybrid-calendar fields for proleptic wire values. */
private[sql] object SqlTimestampConversion:
  // Spark's rebasing tables have no differences at or after 1900-01-01T00:00:00Z.
  private val lastRebaseInstant = Instant.parse("1900-01-01T00:00:00Z")
  private val julianEnd = LocalDate.of(1582, 10, 4)
  private val gregorianStart = LocalDate.of(1582, 10, 15)

  // The ten missing hybrid-calendar dates all map to the first Gregorian date in Spark.
  private def normalizeCutoverDate(date: LocalDate): LocalDate =
    if date.isAfter(julianEnd) && date.isBefore(gregorianStart) then gregorianStart
    else date

  /** Applies Spark's cutover normalization before building a SQL date. */
  def toJavaDate(date: LocalDate): Date = Date.valueOf(normalizeCutoverDate(date))

  /** Mirrors SparkDateTimeUtils.toJavaTimestamp, including historical JVM zone offsets. */
  def toJavaTimestamp(instant: Instant): Timestamp =
    if !instant.isBefore(lastRebaseInstant) then Timestamp.from(instant)
    else
      val zone = TimeZone.getDefault
      val zoneId = zone.toZoneId
      val zoned = instant.atZone(zoneId)
      val local = zoned.toLocalDateTime
      val fields = LocalDateTime.of(normalizeCutoverDate(local.toLocalDate), local.toLocalTime)
      val calendar = new Calendar.Builder()
        .setCalendarType("gregory")
        .setDate(fields.getYear, fields.getMonthValue - 1, fields.getDayOfMonth)
        .setTimeOfDay(fields.getHour, fields.getMinute, fields.getSecond)
        .setTimeZone(zone)
        .build()
      Option(zoneId.getRules.getTransition(fields))
        .filter(_.isOverlap)
        .foreach: transition =>
          val adjacent = calendar.clone().asInstanceOf[Calendar]
          val shift = if transition.getOffsetBefore == zoned.getOffset then -1 else 1
          adjacent.add(Calendar.DAY_OF_MONTH, shift)
          calendar.set(Calendar.ZONE_OFFSET, adjacent.get(Calendar.ZONE_OFFSET))
          calendar.set(Calendar.DST_OFFSET, adjacent.get(Calendar.DST_OFFSET))
      val result = new Timestamp(calendar.getTimeInMillis)
      result.setNanos((fields.getNano / 1000) * 1000)
      result

  /** Mirrors SparkDateTimeUtils.fromJavaTimestamp for Arrow batches and SQL literals. */
  def toInstant(timestamp: Timestamp): Instant =
    val instant = timestamp.toInstant
    if !instant.isBefore(lastRebaseInstant) then instant
    else
      val zone = TimeZone.getDefault
      val calendar = new Calendar.Builder()
        .setCalendarType("gregory")
        .setInstant(timestamp.getTime)
        .setTimeZone(zone)
        .build()
      // Adding the day separately also handles Julian leap days absent in Gregorian years.
      val local = LocalDateTime
        .of(
          calendar.get(Calendar.YEAR),
          calendar.get(Calendar.MONTH) + 1,
          1,
          calendar.get(Calendar.HOUR_OF_DAY),
          calendar.get(Calendar.MINUTE),
          calendar.get(Calendar.SECOND),
          (timestamp.getNanos / 1000) * 1000
        )
        .`with`(ChronoField.ERA, calendar.get(Calendar.ERA))
        .plusDays(calendar.get(Calendar.DAY_OF_MONTH) - 1)
      val zoneId = zone.toZoneId
      val zoned = local.atZone(zoneId)
      val adjusted =
        Option(zoneId.getRules.getTransition(local))
          .filter(_.isOverlap)
          .fold(zoned): _ =>
            val standardOffset = calendar.get(Calendar.ZONE_OFFSET)
            val daylightOffset = calendar.get(Calendar.DST_OFFSET)
            calendar.add(Calendar.DAY_OF_MONTH, 1)
            if standardOffset == calendar.get(Calendar.ZONE_OFFSET) &&
              daylightOffset == calendar.get(Calendar.DST_OFFSET)
            then zoned.withLaterOffsetAtOverlap()
            else zoned.withEarlierOffsetAtOverlap()
      adjusted.toInstant
