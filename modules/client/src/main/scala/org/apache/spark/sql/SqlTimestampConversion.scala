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

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime}
import java.time.temporal.ChronoField
import java.util.{Calendar, TimeZone}

/** Spark's SQL timestamps preserve local fields across the hybrid and proleptic calendars. */
private[sql] object SqlTimestampConversion:
  // Spark's rebasing tables have no differences at or after 1900-01-01T00:00:00Z.
  private val lastRebaseInstant = Instant.parse("1900-01-01T00:00:00Z")
  private val julianEnd = LocalDateTime.of(1582, 10, 4, 23, 59, 59, 999999999)
  private val gregorianStart = LocalDate.of(1582, 10, 15)

  /** Mirrors SparkDateTimeUtils.toJavaTimestamp, including historical JVM zone offsets. */
  def toJavaTimestamp(instant: Instant): Timestamp =
    if !instant.isBefore(lastRebaseInstant) then Timestamp.from(instant)
    else
      val zone = TimeZone.getDefault
      val zoned = instant.atZone(zone.toZoneId)
      val local = zoned.toLocalDateTime
      // Spark maps dates in the Gregorian cutover gap to October 15.
      val fields =
        if local.isAfter(julianEnd) && local.isBefore(gregorianStart.atStartOfDay()) then
          LocalDateTime.of(gregorianStart, local.toLocalTime)
        else local
      val calendar = new Calendar.Builder()
        .setCalendarType("gregory")
        .setDate(fields.getYear, fields.getMonthValue - 1, fields.getDayOfMonth)
        .setTimeOfDay(fields.getHour, fields.getMinute, fields.getSecond)
        .setTimeZone(zone)
        .build()
      val transition = zone.toZoneId.getRules.getTransition(fields)
      if transition != null && transition.isOverlap then
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
      val zoned = local.atZone(zone.toZoneId)
      val transition = zone.toZoneId.getRules.getTransition(local)
      val adjusted =
        if transition != null && transition.isOverlap then
          val standardOffset = calendar.get(Calendar.ZONE_OFFSET)
          val daylightOffset = calendar.get(Calendar.DST_OFFSET)
          calendar.add(Calendar.DAY_OF_MONTH, 1)
          if standardOffset == calendar.get(Calendar.ZONE_OFFSET) &&
            daylightOffset == calendar.get(Calendar.DST_OFFSET)
          then zoned.withLaterOffsetAtOverlap()
          else zoned.withEarlierOffsetAtOverlap()
        else zoned
      adjusted.toInstant
