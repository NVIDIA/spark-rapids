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

  private val NS = "NS"
  private val PS = "PS"
  private val PSPART = "PS*"
  private val SPART = "S*"
  // configured off
  private val CO = "CO"
  private val NA = "NA"

  private val DEFAULT_DS_FILE = "supportedDataSource.csv"
  // var for testing purposes
  private var dsFile = DEFAULT_DS_FILE


  // map of file format => Map[support category => Seq[Datatypes for that category]]
  // contains the details of formats to which ones have datatypes not supported,
  // partially supported or S*
  // var for testing puposes
  private var formatsToSupportedCategory = readSupportedTypesForPlugin

  // for testing purposes only
  def setPluginDataSourceFile(filePath: String): Unit = {
    dsFile = filePath
    formatsToSupportedCategory = readSupportedTypesForPlugin
  }

  // file format should be like this:
  // Format,Direction,BOOLEAN,BYTE,SHORT,INT,LONG,FLOAT,DOUBLE,DATE,...
  // CSV,read,S,S,S,S,S,S,S,S,S*,S,NS,NA,NS,NA,NA,NA,NA,NA
  private def readSupportedTypesForPlugin: Map[String, Map[String, Seq[String]]] = {
    // get the types the Rapids Plugin supports
    val allSupportedReadSources = HashMap.empty[String, Map[String, Seq[String]]]
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
          val nsTypes = dataTypesToSup.filter { case (_, sup) =>
            sup.equals(NA) || sup.equals(NS) || sup.equals(CO)
          }.keys.toSeq.map(_.toLowerCase)
          val allNsTypes = nsTypes.flatMap(t => getOtherTypes(t) :+ t)
          val allBySup = HashMap(NS -> allNsTypes)
          allSupportedReadSources.put(format, allBySup.toMap)
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

  // Parsing the schema string is very complex when you get into nested types, so for now
  // we do the simpler thing of checking to see if the schema string contains types we
  // don't support.
  // NOTE, UDT doesn't show up in the event log, when its written, it gets written as
  // other types since parquet/orc has to know about it
  def scoreReadDataTypes(format: String, schema: String): Double = {
    val schemaLower = schema.toLowerCase
    logWarning("data source is: " + format + " all sploit is: " + formatsToSupportedCategory)
    val formatInLower = format.toLowerCase
    val typesBySup = formatsToSupportedCategory.get(formatInLower)
    val score = typesBySup match {
      case Some(dtSupMap) =>
        // check if any of the not supported types are in the schema
        if (dtSupMap(NS).exists(t => schemaLower.contains(t.toLowerCase()))) {
          0.0
        } else {
          // started out giving different weights based on partial support and so forth
          // but decided to be optimistic and not penalize if we don't know, perhaps
          // make it smarter later.
          // schema could also be incomplete, but similarly don't penalize since we don't
          // know.
          1.0
        }
      case None =>
        // assume we don't support that format
        0.0
    }
    logWarning(s"read score is $score")
    score
  }
}
