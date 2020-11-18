/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.time.LocalDate

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{localDateToDays, SQLDate}

/**
 * Class for helper functions for Date
 */
object DateUtils {
  val unsupportedCharacter = Set(
    'k', 'K','z', 'V', 'c', 'F', 'W', 'Q', 'q', 'G', 'A', 'n', 'N',
    'O', 'X', 'p', '\'', '[', ']', '#', '{', '}', 'Z', 'w', 'e', 'E', 'x', 'Z', 'Y')

  val unsupportedWord = Set(
    "u", "uu", "uuu", "uuuu", "uuuuu", "uuuuuu", "uuuuuuu", "uuuuuuuu", "uuuuuuuuu", "uuuuuuuuuu",
    "y", "yyy", "yyyyy", "yyyyyy", "yyyyyyy", "yyyyyyyy", "yyyyyyyyy", "yyyyyyyyyy",
    "D", "DD", "DDD", "s", "m", "H", "h", "M", "MMM", "MMMM", "MMMMM", "L", "LLL", "LLLL", "LLLLL",
    "d", "S", "SS", "SSS", "SSSS", "SSSSS", "SSSSSSSSS", "SSSSSSS", "SSSSSSSS")

  val conversionMap = Map(
    "MM" -> "%m", "LL" -> "%m", "dd" -> "%d", "mm" -> "%M", "ss" -> "%S", "HH" -> "%H",
    "yy" -> "%y", "yyyy" -> "%Y", "SSSSSS" -> "%f")

  val ONE_SECOND_MICROSECONDS = 1000000

  val ONE_DAY_SECONDS = 86400L

  val ONE_DAY_MICROSECONDS = 86400000000L

  val EPOCH = "epoch"
  val NOW = "now"
  val TODAY = "today"
  val YESTERDAY = "yesterday"
  val TOMORROW = "tomorrow"

  def specialDatesDays: Map[String, Int] = {
    val today = currentDate()
    Map(
      EPOCH -> 0,
      NOW -> today,
      TODAY -> today,
      YESTERDAY -> (today - 1),
      TOMORROW -> (today + 1)
    )
  }

  def specialDatesSeconds: Map[String, Long] = {
    val today = currentDate()
    val now = DateTimeUtils.currentTimestamp()
    Map(
      EPOCH -> 0,
      NOW -> now / 1000000L,
      TODAY -> today * ONE_DAY_SECONDS,
      YESTERDAY -> (today - 1) * ONE_DAY_SECONDS,
      TOMORROW -> (today + 1) * ONE_DAY_SECONDS
    )
  }

  def specialDatesMicros: Map[String, Long] = {
    val today = currentDate()
    val now = DateTimeUtils.currentTimestamp()
    Map(
      EPOCH -> 0,
      NOW -> now,
      TODAY -> today * ONE_DAY_MICROSECONDS,
      YESTERDAY -> (today - 1) * ONE_DAY_MICROSECONDS,
      TOMORROW -> (today + 1) * ONE_DAY_MICROSECONDS
    )
  }

  def currentDate(): SQLDate = localDateToDays(LocalDate.now())

  case class FormatKeywordToReplace(word: String, startIndex: Int, endIndex: Int)

  /**
   * This function converts a java time format string to a strf format string
   * Supports %m,%p,%j,%d,%I,%H,%M,%S,%y,%Y,%f format specifiers.
   * %d Day of the month: 01-31
   * %m Month of the year: 01-12
   * %y Year without century: 00-99c
   * %Y Year with century: 0001-9999
   * %H 24-hour of the day: 00-23
   * %M Minute of the hour: 00-59
   * %S Second of the minute: 00-59
   * %f 6-digit microsecond: 000000-999999
   *
   * reported bugs
   * https://github.com/rapidsai/cudf/issues/4160 after the bug is fixed this method
   * should also support
   * "hh" -> "%I" (12 hour clock)
   * "a" -> "%p" ('AM', 'PM')
   * "DDD" -> "%j" (Day of the year)
   */
  def toStrf(format: String): String = {
    val javaPatternsToReplace = identifySupportedFormatsToReplaceElseThrow(format)
    replaceFormats(format, javaPatternsToReplace)
  }

  def replaceFormats(
      format: String,
      javaPatternsToReplace: ListBuffer[FormatKeywordToReplace]): String = {
    val strf = new StringBuilder(format.length).append(format)
    for (pattern <- javaPatternsToReplace.reverse) {
      if (conversionMap.contains(pattern.word)) {
        strf.replace(pattern.startIndex, pattern.endIndex, conversionMap(pattern.word))
      }
    }
    strf.toString
  }

  def identifySupportedFormatsToReplaceElseThrow(
      format: String): ListBuffer[FormatKeywordToReplace] = {
    var sb = new StringBuilder()
    var index = 0;
    val patterns = new ListBuffer[FormatKeywordToReplace]
    format.map(character => {
      // We are checking to see if this char is a part of a previously read pattern
      // or start of a new one.
      if (sb.length == 0 || sb.charAt(sb.length - 1) == character) {
        if (unsupportedCharacter(character)) {
          throw TimestampFormatConversionException(s"Unsupported character: $character")
        }
        sb.append(character)
      } else {
        // its a new pattern, check if the previous pattern was supported. If its supported,
        // add it to the groups and add this char as a start of a new pattern else throw exception
        val word = sb.toString
        if (unsupportedWord(word)) {
          throw TimestampFormatConversionException(s"Unsupported word: $word")
        }
        val startIndex = index - word.length
        patterns += FormatKeywordToReplace(word, startIndex, startIndex + word.length)
        sb = new StringBuilder(format.length)
        if (unsupportedCharacter(character)) {
          throw TimestampFormatConversionException(s"Unsupported character: $character")
        }
        sb.append(character)
      }
      index = index + 1
    })
    if (sb.length > 0) {
      val word = sb.toString()
      if (unsupportedWord(word)) {
        throw TimestampFormatConversionException(s"Unsupported word: $word")
      }
      val startIndex = format.length - word.length
      patterns += FormatKeywordToReplace(word, startIndex, startIndex + word.length)
    }
    patterns
  }

  case class TimestampFormatConversionException(reason: String) extends Exception
}
