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
          allSupportedReadSources(format) = header.drop(2).zip(cols.drop(2)).toMap
        }
      }
    } finally {
      source.close()
    }
    allSupportedReadSources.toMap
  }

  def scoreReadDataTypes(format: String, schema: String, schemaIncomplete: Boolean): Double = {
    logWarning("data source is: " + format)
    val formatInLower = format.toLowerCase
    val score = if (allSupportedReadSources.contains(formatInLower)) {
      logWarning(s"data source format ${formatInLower} is supported by plugin")
      val readSchema = schema.split(",").map(_.toLowerCase)
      val scores = readSchema.map { typeRead =>
        // TODO - need to add array/map/etc
        val cType = typeRead match {
          case "bigint" => "long"
          case "smallint" => "short"
          case "integer" => "int"
          case "tinyint" => "byte"
          case "real" => "float"
          case "dec" | "numeric" => "decimal"
          case "interval" => "calendar"
          case other => other
        }
        if (allSupportedReadSources(formatInLower).contains(cType)) {
          val supString = allSupportedReadSources(formatInLower).getOrElse(cType, "")
          logWarning(s"type is : $typeRead -> $cType supported is: $supString")
          supString match {
            case "S" => 1.0
            case "S*" =>
              // decimals or timestamps
              // since don't know be conservative and
              if (typeRead.equals("decimal")) {
                0.0
              } else {
                // timestamps
                0.5
              }
            case "PS" => 0.5
            case "PS*" =>
              // parquet - PS* (missing nested BINARY, ARRAY, MAP, UDT)
              0.5
            case "NS" => 0.0
            case "NA" => 0.0
            case unknown =>
              logWarning(s"unknown type $unknown for type: $typeRead")
              0.0
          }
        } else {
          // datatype not supported
          0.0
        }
      }
      if (scores.contains(0.0)) {
        0.0
      } else {
        if (schemaIncomplete) {
          // we don't know for sure if the other types being read are supported
          // add one more score of 0.5 in to bring it down a little
          (scores.sum + 0.5) / (scores.size + 1)
        } else {
          scores.sum / scores.size
        }
      }
    } else {
      // format not supported
      0.0
    }
    score
  }
}
