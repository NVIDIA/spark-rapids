/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.io.{BufferedSource, Source}

import org.apache.spark.internal.Logging

/**
 * This class is used to check what the RAPIDS Accelerator for Apache Spark
 * supports for data formats and data types.
 * By default it relies on a csv file included in the jar which is generated
 * by the plugin which lists the formats and types supported.
 */
class PluginTypeChecker extends Logging {

  private val NS = "NS"
  private val PS = "PS"
  private val PSPART = "PS*"
  private val SPART = "S*"
  // configured off
  private val CO = "CO"
  private val NA = "NA"

  private val DEFAULT_DS_FILE = "supportedDataSource.csv"
  private val OPERATORS_SCORE_FILE = "operatorsScore.csv"
  private val SUPPORTED_EXECS_FILE = "supportedExecs.csv"
  private val SUPPORTED_EXPRS_FILE = "supportedExprs.csv"

  // map of file format => Map[support category => Seq[Datatypes for that category]]
  // contains the details of formats to which ones have datatypes not supported.
  // Write formats contains only the formats that are supported. Types cannot be determined
  // from event logs for write formats.
  // var for testing purposes
  private var (readFormatsAndTypes, writeFormats) = readSupportedTypesForPlugin

  private var supportedOperatorsScore = readOperatorsScore

  private var supportedExecs = readSupportedExecs

  private var supportedExprs = readSupportedExprs

  // for testing purposes only
  def setPluginDataSourceFile(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    val (readFormatsAndTypesTest, writeFormatsTest) = readSupportedTypesForPlugin(source)
    readFormatsAndTypes = readFormatsAndTypesTest
    writeFormats = writeFormatsTest
  }

  def setOperatorScore(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    supportedOperatorsScore = readSupportedOperators(source).map(x => (x._1, x._2.toDouble))
  }

  def setSupportedExecs(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    // We are reading only first 2 columns for now and other columns are ignored intentionally.
    supportedExecs = readSupportedOperators(source)
  }

  def setSupportedExprs(filePath: String): Unit = {
    val source = Source.fromFile(filePath)
    // We are reading only first 2 columns for now and other columns are ignored intentionally.
    supportedExprs = readSupportedOperators(source)
  }

  def getSupportedExprs: Map[String, String] = supportedExprs

  private def readOperatorsScore: Map[String, Double] = {
    val source = Source.fromResource(OPERATORS_SCORE_FILE)
    readSupportedOperators(source, "score").map(x => (x._1, x._2.toDouble))
  }

  private def readSupportedExecs: Map[String, String] = {
    val source = Source.fromResource(SUPPORTED_EXECS_FILE)
    readSupportedOperators(source)
  }

  private def readSupportedExprs: Map[String, String] = {
    val source = Source.fromResource(SUPPORTED_EXPRS_FILE)
    // Some SQL function names have backquotes(`) around their names,
    // so we remove them before saving.
    readSupportedOperators(source, "exprs").map(
      x => (x._1.toLowerCase.replaceAll("\\`", ""), x._2))
  }

  private def readSupportedTypesForPlugin: (
      Map[String, Map[String, Seq[String]]], ArrayBuffer[String]) = {
    val source = Source.fromResource(DEFAULT_DS_FILE)
    readSupportedTypesForPlugin(source)
  }

  // operatorType can be exprs, score or execs(default). Reads the columns in file depending
  // on the operatorType passed to this function.
  private def readSupportedOperators(source: BufferedSource,
      operatorType: String = "execs"): Map[String, String] = {
    val supportedOperators = HashMap.empty[String, String]
    try {
      val fileContents = source.getLines().toSeq
      if (fileContents.size < 2) {
        throw new IllegalStateException(s"${source.toString} file appears corrupt," +
            " must have at least the header and one line")
      }
      // first line is header
      val header = fileContents.head.split(",").map(_.toLowerCase)
      // the rest of the rows are operators with additional info
      fileContents.tail.foreach { line =>
        val cols = line.split(",")
        if (header.size != cols.size) {
          throw new IllegalStateException(s"${source.toString} file appears corrupt," +
              s" header length doesn't match rows length. Row that doesn't match is " +
              s"${cols.mkString(",")}")
        }
        // There are addidtional checks for Expressions. In physical plan, SQL function name is
        // printed instead of expression name. We have to save both expression name and
        // SQL function name(if there is one) so that we don't miss the expression while
        // parsing the execs.
        // Ex: Expression name = Substring, SQL function= `substr`; `substring`
        // Ex: Expression name = Average, SQL function name = `avg`
        if (operatorType.equals("exprs")) {
          // save expression name
          supportedOperators.put(cols(0), cols(1))
          // Check if there is SQL function name for the above expression
          if (cols(2).nonEmpty && cols(2) != None) {
            // Split on `;` if there are multiple SQL names as shown in above example and
            // save each SQL function name as a separate key.
            val sqlFuncNames = cols(2).split(";")
            for (i <- sqlFuncNames) {
              supportedOperators.put(i, cols(1))
            }
          }
        } else {
          supportedOperators.put(cols(0), cols(1))
        }
      }
    } finally {
      source.close()
    }
    supportedOperators.toMap
  }

  // file format should be like this:
  // Format,Direction,BOOLEAN,BYTE,SHORT,INT,LONG,FLOAT,DOUBLE,DATE,...
  // CSV,read,S,S,S,S,S,S,S,S,S*,S,NS,NA,NS,NA,NA,NA,NA,NA
  // Parquet,write,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA
  private def readSupportedTypesForPlugin(
      source: BufferedSource): (Map[String, Map[String, Seq[String]]], ArrayBuffer[String]) = {
    // get the types the Rapids Plugin supports
    val allSupportedReadSources = HashMap.empty[String, Map[String, Seq[String]]]
    val allSupportedWriteFormats = ArrayBuffer[String]()
    try {
      val fileContents = source.getLines().toSeq
      if (fileContents.size < 2) {
        throw new IllegalStateException("supportedDataSource file appears corrupt," +
          " must have at least the header and one line")
      }
      // first line is header
      val header = fileContents.head.split(",").map(_.toLowerCase)
      // the rest of the rows are file formats with type supported info
      fileContents.tail.foreach { line =>
        val cols = line.split(",")
        if (header.size != cols.size) {
          throw new IllegalStateException("supportedDataSource file appears corrupt," +
            " header length doesn't match rows length")
        }
        val format = cols(0).toLowerCase
        val direction = cols(1).toLowerCase()
        if (direction.equals("read")) {
          val dataTypesToSup = header.drop(2).zip(cols.drop(2)).toMap
          val nsTypes = dataTypesToSup.filter { case (_, sup) =>
            sup.equals(NA) || sup.equals(NS) || sup.equals(CO)
          }.keys.toSeq.map(_.toLowerCase)
          val allNsTypes = nsTypes.flatMap(t => getOtherTypes(t) :+ t)
          val allBySup = HashMap(NS -> allNsTypes)
          allSupportedReadSources.put(format, allBySup.toMap)
        } else if (direction.equals("write")) {
          allSupportedWriteFormats += format
        }
      }
    } finally {
      source.close()
    }
    (allSupportedReadSources.toMap, allSupportedWriteFormats)
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
  def scoreReadDataTypes(format: String, schema: String): (Double, Set[String]) = {
    val schemaLower = schema.toLowerCase
    val formatInLower = format.toLowerCase
    val typesBySup = readFormatsAndTypes.get(formatInLower)
    val score = typesBySup match {
      case Some(dtSupMap) =>
        // check if any of the not supported types are in the schema
        val nsFiltered = dtSupMap(NS).filter(t => schemaLower.contains(t.toLowerCase()))
        if (nsFiltered.nonEmpty) {
          (0.0, nsFiltered.toSet)
        } else {
          // Started out giving different weights based on partial support and so forth
          // but decided to be optimistic and not penalize if we don't know, perhaps
          // make it smarter later.
          // Schema could also be incomplete, but similarly don't penalize since we don't
          // know.
          (1.0, Set.empty[String])
        }
      case None =>
        // assume we don't support that format
        (0.0, Set("*"))
    }
    score
  }

  def getWriteFormatString(node: String): String = {
    // We need to parse the input string to get the write format. Write format is either third
    // or fourth parameter in the input string. If the partition columns is provided, then the
    // write format will be the fourth parameter.
    // Example string in the eventlog:
    // Execute InsertIntoHadoopFsRelationCommand
    // gs://08f3844/, false, [op_cmpny_cd#25, clnt_rq_sent_dt#26], ORC, Map(path -> gs://08f3844)
    val parsedString = node.split(",", 3).last.trim // remove first 2 parameters from the string
    if (parsedString.startsWith("[")) {
      // Optional parameter is present in the eventlog. Get the fourth parameter by skipping the
      // optional parameter string.
      parsedString.split("(?<=],)").map(_.trim).slice(1, 2)(0).split(",")(0)
    } else {
      parsedString.split(",")(0) // return third parameter from the input string
    }
  }

  def isWriteFormatsupported(writeFormat: String): Boolean = {
    val format = writeFormat.toLowerCase.trim
    writeFormats.map(x => x.trim).contains(format)
  }

  def isWriteFormatsupported(writeFormat: ArrayBuffer[String]): ArrayBuffer[String] = {
    writeFormat.map(x => x.toLowerCase.trim).filterNot(
      writeFormats.map(x => x.trim).contains(_))
  }

  def getSpeedupFactor(execOrExpr: String): Double = {
    supportedOperatorsScore.get(execOrExpr).getOrElse(-1)
  }

  def isExecSupported(exec: String): Boolean = {
    // special case ColumnarToRow and assume it will be removed or will we replace
    // with GPUColumnarToRow. TODO - we can add more logic here to look at operator
    //  before and after
    if (exec == "ColumnarToRow") {
      return true
    }
    if (supportedExecs.contains(exec)) {
      val execSupported = supportedExecs.getOrElse(exec, "NS")
      if (execSupported == "S") {
        true
      } else {
        logDebug(s"Exec explicitly not supported, value: $execSupported")
        false
      }
    } else {
      logDebug(s"Exec $exec does not exist in supported execs file")
      false
    }
  }

  def isExprSupported(expr: String): Boolean = {
    // Remove _ from the string. Example: collect_list => collectlist.
    // collect_list is alias for CollectList aggregate function
    val exprLowercase = expr.toLowerCase.replace("_","")
    if (supportedExprs.contains(exprLowercase)) {
      val exprSupported = supportedExprs.getOrElse(exprLowercase, "NS")
      if (exprSupported == "S") {
        true
      } else {
        logDebug(s"Expression explicitly not supported, value: $exprSupported")
        false
      }
    } else {
      logDebug(s"Expr $expr does not exist in supported execs file")
      false
    }
  }
}
