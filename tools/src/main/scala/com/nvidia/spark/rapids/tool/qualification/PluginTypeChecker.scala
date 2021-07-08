/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.qualification

import scala.collection.mutable.HashMap
import scala.io.Source

import org.apache.spark.internal.Logging

class PluginTypeChecker extends Logging {

  // map of file format => Map[datatype => supported string]
  val allSplit = new HashMap[String, Map[String, Seq[String]]]()
  val allSupportedReadSources = readSupportedTypesForPlugin

  // file format should be like this:
  // Format,Direction,BOOLEAN,BYTE,SHORT,INT,LONG,FLOAT,DOUBLE,DATE,...
  // CSV,read,S,S,S,S,S,S,S,S,S*,S,NS,NA,NS,NA,NA,NA,NA,NA
  private def readSupportedTypesForPlugin: Map[String, Map[String, String]] = {
    // get the types the Rapids Plugin supports
    val allSupportedReadSources = HashMap.empty[String, Map[String, String]]
    val dsFile = "supportedDataSource.csv"
    val source = Source.fromResource(dsFile)
    try {
      val fileContents = source.getLines().toSeq
      // first line is header
      val header = fileContents.head.split(",").map(_.toLowerCase)
      // the rest of the rows are file formats with type supported info
      fileContents.tail.foreach { line =>
        val cols = line.split(",")
        if (header.size != cols.size) {
          logError("something went wrong, header is not same size as cols")
          throw new IllegalStateException("supportedDataSource file appears corrupt," +
            " header length doesn't match rows length")
        }
        val format = cols(0).toLowerCase
        val direction = cols(1).toLowerCase()
        if (direction.equals("read")) {
          val dataTypesToSup = header.drop(2).zip(cols.drop(2)).toMap
          logWarning("datatypes to sup are: " + dataTypesToSup)
          val nsTypes = dataTypesToSup.filter { case (dt, sup) =>
            sup.equals("NA") || sup.equals("NS") || sup.equals("CO")
          }.keys.toSeq
          val allNsTypes = nsTypes.flatMap { t =>
            getOtherTypes(t) :+ t
          }
          val psTypes = dataTypesToSup.filter { case (dt, sup) =>
            sup.equals("PS") || sup.equals("PS*")
          }.keys.toSeq
          val allPsTypes = psTypes.flatMap { t =>
            getOtherTypes(t) :+ t
          }
          val sPartTypes = dataTypesToSup.filter { case (dt, sup) =>
            // we care for decimal
            sup.equals("S*")
          }.keys.toSeq
          val allsPartTypes = sPartTypes.flatMap { t =>
            getOtherTypes(t) :+ t
          }
          val allBySup = HashMap("NS" -> allNsTypes, "PS" -> allPsTypes, "S*" -> allsPartTypes)
          allSplit.put(format, allBySup.toMap)
          allSupportedReadSources(format) = dataTypesToSup
        }
      }
    } finally {
      source.close()
    }
    allSupportedReadSources.toMap
  }

  def getOtherTypes(typeRead: String): Seq[String] = {
    typeRead match {
      case "long" => Seq("bigint")
      case "short" => Seq("smallint")
      case "int" => Seq("integer")
      case "byte" => Seq("tinyint")
      case "float" => Seq("real")
      case "decimal" => Seq("dec", "numeric")
      case "calendar" => Seq("interval")
      case other => Seq.empty[String]
    }
  }

  // NOTE, UDT doesn't show up in the event log, when its written, it gets written as
  // other types since parquet/orc has to know about it
  def scoreReadDataTypes(format: String, schema: String): Double = {
    logWarning("data source is: " + format + " all sploit is: " + allSplit)
    val formatInLower = format.toLowerCase
    val score = if (allSupportedReadSources.contains(formatInLower)) {
      logWarning(s"data source format ${formatInLower} is supported by plugin")
      val typesBySup = allSplit(formatInLower)
      val hasNotSupported = typesBySup("NS").exists(schema.contains(_))
      logWarning(s"has not supported is: $hasNotSupported")
      val formatScore = if (hasNotSupported) {
        0.0
      } else {
        val hasSupportedPart = typesBySup("S*").exists(schema.contains(_))
        logWarning(s"has supported partial supported is: $hasSupportedPart")
        if (hasSupportedPart) {
          // timestamps or decimals, check for decimals specifically
          // could look for decimal(4,2) precision but might have operators
          // that make it worse so just assume worst case
          if (schema.contains("decimal")) {
            0.0
          } else {
            1.0
          }
        } else {
          val hasPartSupported = typesBySup("PS").exists(schema.contains(_))
          logWarning(s"has part supported is: $hasPartSupported")
          if (hasPartSupported) {
            // TODO - is this to pesimistic?
            0.75
          } else {
            // either supported or we don't know
            1.0
          }
        }
      }

      if (formatScore == 0.0) {
        0.0
      } else {
        // schema could be incomplete but just report what we were able to parse.
        // Instead could mark some off but if the rest is fine we penalize for nothiner.
        formatScore
      }
    } else {
      // format not supported
      0.0
    }
    logWarning(s"read score is $score")
    score
  }
}
