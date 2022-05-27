/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import scala.collection.mutable.HashMap

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

case class ReadMetaData(schema: String, location: String, filters: String, format: String)

object ReadParser extends Logging {

  // strip off the struct<> part that Spark adds to the ReadSchema
  def formatSchemaStr(schema: String): String = {
    schema.stripPrefix("struct<").stripSuffix(">")
  }

  // This tries to get just the field specified by tag in a string that
  // may contain multiple fields.  It looks for a comma to delimit fields.
  private def getFieldWithoutTag(str: String, tag: String): String = {
    val index = str.indexOf(tag)
    // remove the tag from the final string returned
    val subStr = str.substring(index + tag.size)
    val endIndex = subStr.indexOf(", ")
    if (endIndex != -1) {
      subStr.substring(0, endIndex)
    } else {
      subStr
    }
  }

  def parseReadNode(node: SparkPlanGraphNode): ReadMetaData = {
    val schemaTag = "ReadSchema: "
    val schema = if (node.desc.contains(schemaTag)) {
      formatSchemaStr(getFieldWithoutTag(node.desc, schemaTag))
    } else {
      ""
    }
    val locationTag = "Location: "
    val location = if (node.desc.contains(locationTag)) {
      getFieldWithoutTag(node.desc, locationTag)
    } else if (node.name.contains("JDBCRelation")) {
      // see if we can report table or query
      val JDBCPattern = raw".*JDBCRelation\((.*)\).*".r
      node.name match {
        case JDBCPattern(tableName) => tableName
        case _ => "unknown"
      }
    } else {
      "unknown"
    }
    val pushedFilterTag = "PushedFilters: "
    val pushedFilters = if (node.desc.contains(pushedFilterTag)) {
      getFieldWithoutTag(node.desc, pushedFilterTag)
    } else {
      "unknown"
    }
    val formatTag = "Format: "
    val fileFormat = if (node.desc.contains(formatTag)) {
      val format = getFieldWithoutTag(node.desc, formatTag)
      if (node.name.startsWith("Gpu")) {
        s"${format}(GPU)"
      } else {
        format
      }
    } else if (node.name.contains("JDBCRelation")) {
      "JDBC"
    } else {
      "unknown"
    }
    ReadMetaData(schema, location, pushedFilters, fileFormat)
  }

  // For the read score we look at the read format and datatypes for each
  // format and for each read give it a value 0.0 - 1.0 depending on whether
  // the format is supported and if the data types are supported. So if none
  // of the data types are supported, the score would be 0.0 and if the format
  // and datatypes are supported the score would be 1.0.
  def calculateReadScoreRatio(meta: ReadMetaData,
      pluginTypeChecker: PluginTypeChecker): Double = {
    val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()
    val (readScore, nsTypes) = pluginTypeChecker.scoreReadDataTypes(meta.format, meta.schema)
    if (nsTypes.nonEmpty) {
      val currentFormat = notSupportFormatAndTypes.get(meta.format).getOrElse(Set.empty[String])
      notSupportFormatAndTypes(meta.format) = (currentFormat ++ nsTypes)
    }
    // TODO - not doing anything with note supported types right now
    readScore
  }
}
