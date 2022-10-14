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

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode}
import org.apache.spark.sql.rapids.tool.{AppBase, BuildSide, JoinType, ToolUtils}

class ExecInfo(
    val sqlID: Long,
    val exec: String,
    val expr: String,
    val speedupFactor: Double,
    val duration: Option[Long],
    val nodeId: Long,
    val isSupported: Boolean,
    val children: Option[Seq[ExecInfo]], // only one level deep
    val stages: Set[Int] = Set.empty,
    val shouldRemove: Boolean = false,
    val unsupportedExprs: Array[String] = Array.empty) {
  private def childrenToString = {
    val str = children.map { c =>
      c.map("       " + _.toString).mkString("\n")
    }.getOrElse("")
    if (str.nonEmpty) {
      "\n" + str
    } else {
      str
    }
  }
  override def toString: String = {
    s"exec: $exec, expr: $expr, sqlID: $sqlID , speedupFactor: $speedupFactor, " +
      s"duration: $duration, nodeId: $nodeId, " +
      s"isSupported: $isSupported, children: " +
      s"${childrenToString}, stages: ${stages.mkString(",")}, " +
      s"shouldRemove: $shouldRemove"
  }
}

case class PlanInfo(
    appID: String,
    sqlID: Long,
    sqlDesc: String,
    execInfo: Seq[ExecInfo]
)

object SQLPlanParser extends Logging {

  def parseSQLPlan(
      appID: String,
      planInfo: SparkPlanInfo,
      sqlID: Long,
      sqlDesc: String,
      checker: PluginTypeChecker,
      app: AppBase): PlanInfo = {
    val planGraph = SparkPlanGraph(planInfo)
    // we want the sub-graph nodes to be inside of the wholeStageCodeGen so use nodes
    // vs allNodes
    val execInfos = planGraph.nodes.flatMap { node =>
      parsePlanNode(node, sqlID, checker, app)
    }
    PlanInfo(appID, sqlID, sqlDesc, execInfos)
  }

  def getStagesInSQLNode(node: SparkPlanGraphNode, app: AppBase): Set[Int] = {
    val nodeAccums = node.metrics.map(_.accumulatorId)
    nodeAccums.flatMap { nodeAccumId =>
      app.accumulatorToStages.get(nodeAccumId)
    }.flatten.toSet
  }

  private val skipUDFCheckExecs = Seq("ArrowEvalPython", "AggregateInPandas",
    "FlatMapGroupsInPandas", "MapInPandas", "WindowInPandas")

  def parsePlanNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase
  ): Seq[ExecInfo] = {
    if (node.name.contains("WholeStageCodegen")) {
      // this is special because it is a SparkPlanGraphCluster vs SparkPlanGraphNode
      WholeStageExecParser(node.asInstanceOf[SparkPlanGraphCluster], checker, sqlID, app).parse
    } else {
      val execInfos = node.name match {
        case "AggregateInPandas" =>
          AggregateInPandasExecParser(node, checker, sqlID).parse
        case "ArrowEvalPython" =>
          ArrowEvalPythonExecParser(node, checker, sqlID).parse
        case "BatchScan" =>
          BatchScanExecParser(node, checker, sqlID, app).parse
        case "BroadcastExchange" =>
          BroadcastExchangeExecParser(node, checker, sqlID, app).parse
        case "BroadcastHashJoin" =>
          BroadcastHashJoinExecParser(node, checker, sqlID).parse
        case "BroadcastNestedLoopJoin" =>
          BroadcastNestedLoopJoinExecParser(node, checker, sqlID).parse
        case "CartesianProduct" =>
          CartesianProductExecParser(node, checker, sqlID).parse
        case "Coalesce" =>
          CoalesceExecParser(node, checker, sqlID).parse
        case "CollectLimit" =>
          CollectLimitExecParser(node, checker, sqlID).parse
        case "ColumnarToRow" =>
          // ignore ColumnarToRow to row for now as assume everything is columnar
          new ExecInfo(sqlID, node.name, expr = "", 1, duration = None, node.id,
            isSupported = false, None, Set.empty, shouldRemove=true)
        case c if (c.contains("CreateDataSourceTableAsSelectCommand")) =>
          // create data source table doesn't show the format so we can't determine
          // if we support it
          new ExecInfo(sqlID, node.name, expr = "", 1, duration = None, node.id,
            isSupported = false, None)
        case "CustomShuffleReader" | "AQEShuffleRead" =>
          CustomShuffleReaderExecParser(node, checker, sqlID).parse
        case "Exchange" =>
          ShuffleExchangeExecParser(node, checker, sqlID, app).parse
        case "Expand" =>
          ExpandExecParser(node, checker, sqlID).parse
        case "Filter" =>
          FilterExecParser(node, checker, sqlID).parse
        case "FlatMapGroupsInPandas" =>
          FlatMapGroupsInPandasExecParser(node, checker, sqlID).parse
        case "Generate" =>
          GenerateExecParser(node, checker, sqlID).parse
        case "GlobalLimit" =>
          GlobalLimitExecParser(node, checker, sqlID).parse
        case "HashAggregate" =>
          HashAggregateExecParser(node, checker, sqlID, app).parse
        case "LocalLimit" =>
          LocalLimitExecParser(node, checker, sqlID).parse
        case "InMemoryTableScan" =>
          InMemoryTableScanExecParser(node, checker, sqlID).parse
        case i if (i.contains("InsertIntoHadoopFsRelationCommand") ||
          i == "DataWritingCommandExec") =>
          DataWritingCommandExecParser(node, checker, sqlID).parse
        case "MapInPandas" =>
          MapInPandasExecParser(node, checker, sqlID).parse
        case "ObjectHashAggregate" =>
          ObjectHashAggregateExecParser(node, checker, sqlID, app).parse
        case "Project" =>
          ProjectExecParser(node, checker, sqlID).parse
        case "Range" =>
          RangeExecParser(node, checker, sqlID).parse
        case "Sample" =>
          SampleExecParser(node, checker, sqlID).parse
        case "ShuffledHashJoin" =>
          ShuffledHashJoinExecParser(node, checker, sqlID, app).parse
        case "Sort" =>
          SortExecParser(node, checker, sqlID).parse
        case s if (s.startsWith("Scan")) =>
          FileSourceScanExecParser(node, checker, sqlID, app).parse
        case "SortAggregate" =>
          SortAggregateExecParser(node, checker, sqlID).parse
        case "SortMergeJoin" =>
          SortMergeJoinExecParser(node, checker, sqlID).parse
        case "SubqueryBroadcast" =>
          SubqueryBroadcastExecParser(node, checker, sqlID, app).parse
        case "TakeOrderedAndProject" =>
          TakeOrderedAndProjectExecParser(node, checker, sqlID).parse
        case "Union" =>
          UnionExecParser(node, checker, sqlID).parse
        case "Window" =>
          WindowExecParser(node, checker, sqlID).parse
        case "WindowInPandas" =>
          WindowInPandasExecParser(node, checker, sqlID).parse
        case _ =>
          new ExecInfo(sqlID, node.name, expr = "", 1, duration = None, node.id,
            isSupported = false, None)
      }
      // check is the node has a dataset operations and if so change to not supported
      val ds = app.isDataSetOrRDDPlan(node.desc)
      // we don't want to mark the *InPandas and ArrowEvalPythonExec as unsupported with UDF
      val containsUDF = if (skipUDFCheckExecs.contains(node.name)) {
        false
      } else {
        app.containsUDF(node.desc)
      }
      val stagesInNode = getStagesInSQLNode(node, app)
      val supported = execInfos.isSupported && !ds && !containsUDF

      Seq(new ExecInfo(execInfos.sqlID, execInfos.exec, execInfos.expr, execInfos.speedupFactor,
        execInfos.duration, execInfos.nodeId, supported, execInfos.children,
        stagesInNode, execInfos.shouldRemove, execInfos.unsupportedExprs))
    }
  }

  /**
   * This function is used to calculate an average speedup factor. The input
   * is assumed to an array of doubles where each element is >= 1. If the input array
   * is empty we return 1 because we assume we don't slow things down. Generally
   * the array shouldn't be empty, but if there is some weird case we don't want to
   * blow up, just say we don't speed it up.
   */
  def averageSpeedup(arr: Seq[Double]): Double = {
    if (arr.isEmpty) {
      1.0
    } else {
      val sum = arr.sum
      ToolUtils.calculateAverage(sum, arr.size, 2)
    }
  }

  /**
   * Get the total duration by finding the accumulator with the largest value.
   * This is because each accumulator has a value and an update. As tasks end
   * they just update the value = value + update, so the largest value will be
   * the duration.
   */
  def getTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val taskForAccum = accumId.flatMap(id => app.taskStageAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = taskForAccum.map(_.value.getOrElse(0L))
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

  def getDriverTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val accums = accumId.flatMap(id => app.driverAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = accums.map(_.value)
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

  private def getFunctionName(functionPattern: Regex, expr: String): Option[String] = {
    val funcName = functionPattern.findFirstMatchIn(expr) match {
      case Some(func) =>
        val func1 = func.group(1)
        // `cast` is not an expression hence should be ignored. In the physical plan cast is
        // usually presented as function call for example: `cast(value#9 as date)`. We add
        // other function names to the result.
        if (!func1.equalsIgnoreCase("cast")) {
          Some(func1)
        } else {
          None
        }
      case _ => logDebug(s"Incorrect expression - $expr")
        None
    }
    funcName
  }

  def parseProjectExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // Project [cast(value#136 as string) AS value#144, CEIL(value#136) AS CEIL(value)#143L]
    // remove the alias names before parsing
    val pattern = """(AS) ([(\w# )]+)""".r
    // This is to split multiple column names in Project. Project may have a function on a column.
    // This will contain array of columns names specified in ProjectExec. Below regex will first
    // remove the alias names from the string followed by a split which produces an array containing
    // column names. Finally we remove the paranthesis from the beginning and end to get only
    // the expressions. Result will be as below.
    // paranRemoved = Array(cast(value#136 as string), CEIL(value#136))
    val paranRemoved = pattern.replaceAllIn(exprStr.replace("),", "::"), "")
        .split(",").map(_.trim).map(_.replaceAll("""^\[+""", "").replaceAll("""\]+$""", ""))
    val functionPattern = """(\w+)\(.*\)""".r
    paranRemoved.foreach { case expr =>
      val functionName = getFunctionName(functionPattern, expr)
      functionName match {
        case Some(func) => parsedExpressions += func
        case _ => // NO OP
      }
    }
    parsedExpressions.distinct.toArray
  }

  // This parser is used for SortAggregateExec, HashAggregateExec and ObjectHashAggregateExec
  def parseAggregateExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // (key=[num#83], functions=[partial_collect_list(letter#84, 0, 0), partial_count(letter#84)])
    val pattern = """functions=\[([\w#, +*\\\-\.<>=\`\(\)]+\])""".r
    val aggregatesString = pattern.findFirstMatchIn(exprStr)
    // This is to split multiple column names in AggregateExec. Each column will be aggregating
    // based on the aggregate function. Here "partial_" is removed and only function name is
    // preserved. Below regex will first remove the "functions=" from the string followed by
    // removing "partial_". That string is split which produces an array containing
    // column names. Finally we remove the parentheses from the beginning and end to get only
    // the expressions. Result will be as below.
    // paranRemoved = Array(collect_list(letter#84, 0, 0),, count(letter#84))
    if (aggregatesString.isDefined) {
      val paranRemoved = aggregatesString.get.toString.replaceAll("functions=", "").
          replaceAll("partial_", "").split("(?<=\\),)").map(_.trim).
          map(_.replaceAll("""^\[+""", "").replaceAll("""\]+$""", ""))
      val functionPattern = """(\w+)\(.*\)""".r
      paranRemoved.foreach { case expr =>
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseWindowExpressions(exprStr:String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // [sum(cast(level#30 as bigint)) windowspecdefinition(device#29, id#28 ASC NULLS FIRST,
    // specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sum#35L,
    // row_number() windowspecdefinition(device#29, id#28 ASC NULLS FIRST, specifiedwindowframe
    // (RowFrame, unboundedpreceding$(), currentrow$())) AS row_number#41], [device#29],
    // [id#28 ASC NULLS FIRST]

    // This splits the string to get only the expressions in WindowExec. So we first split the
    // string on closing bracket ] and get the first element from the array. This is followed
    // by removing the first and last parenthesis and removing the cast as it is not an expr.
    // Lastly we split the string by keyword windowsspecdefinition so that each array element
    // except the last element contains one window aggregate function.
    // sum(level#30 as bigint))
    // (device#29, id#28 ASC NULLS FIRST, .....  AS sum#35L, row_number()
    // (device#29, id#28 ASC NULLS FIRST, ......  AS row_number#41
    val windowExprs = exprStr.split("(?<=\\])")(0).
        trim.replaceAll("""^\[+""", "").replaceAll("""\]+$""", "").
        replaceAll("cast\\(", "").split("windowspecdefinition").map(_.trim)
    val functionPattern = """(\w+)\(""".r

    // Get functionname from each array element except the last one as it doesn't contain
    // any window function
    for ( i <- 0 to windowExprs.size - 1 ) {
      val windowFunc = functionPattern.findAllIn(windowExprs(i)).toList
      val expr = windowFunc(windowFunc.size -1)
      val functionName = getFunctionName(functionPattern, expr)
      functionName match {
        case Some(func) => parsedExpressions += func
        case _ => // NO OP
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseExpandExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // [List(x#1564, hex(y#1455L)#1565, CEIL(z#1456)#1566L, 0),
    // List(x#1564, hex(y#1455L)#1565, null, 1), .......
    // , spark_grouping_id#1567L]
    val pattern = """\[List([\w#, \(\)]+0\),)""".r
    val expandString = pattern.findFirstMatchIn(exprStr)
    // This splits the string to get expressions within ExpandExec. With ExpandExec,
    // there are multiple rows in the output for single input row. This is shown in
    // physical plan by appending list of different output rows. We can extract all
    // expressions from the first index of the list. So we split the string on
    // first index and remove parenthesis and the resultant array contains expressions
    // in this exec. Result will be as shown below:
    // Array(x#1712, hex(y#1701L)#1713)
    if (expandString.isDefined) {
      val firstIndexElements = expandString.get.toString.split("0\\),").mkString.trim
      val parenRemoved = firstIndexElements.split(",").map(
        _.trim).map(_.replaceAll("""^\[List\(""", ""))
      val functionPattern = """(\w+)\(.*\)""".r

      parenRemoved.foreach { case expr =>
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseTakeOrderedExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // (limit=2, orderBy=[FLOOR(z#796) ASC NULLS FIRST,
    // CEIL(y#795L) ASC NULLS FIRST,y#1588L ASC NULLS FIRST], output=[x#794,y#796L,z#795])
    val pattern = """orderBy=\[([\w#, \(\)]+\])""".r
    val orderString = pattern.findFirstMatchIn(exprStr)
    // This is to split multiple column names in orderBy clause of parse TakeOrderedAndProjectExec.
    // First we remove orderBy from the string and then split the resultant string.
    // The string is split on delimiter containing FIRST, OR LAST, which is the last string
    // of each column in this Exec that produces an array containing
    // column names. Finally we remove the parentheses from the beginning and end to get only
    // the expressions. Result will be as below.
    // Array(FLOOR(z#796) ASC NULLS FIRST,, CEIL(y#795L) ASC NULLS FIRST)
    if (orderString.isDefined) {
      val parenRemoved = orderString.get.toString.replaceAll("orderBy=", "").
        split("(?<=FIRST,)|(?<=LAST,)").map(_.trim).map(
        _.replaceAll("""^\[+""", "").replaceAll("""\]+$""", ""))
      val functionPattern = """(\w+)\(.*\)""".r
      parenRemoved.foreach { case expr =>
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseGenerateExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // Only one generator is allowed per select clause. So we need to parse first expression in
    // the GenerateExec and check if it is supported.
    // 1. Generate explode(arrays#1306), [id#1304], true, [col#1426]
    // 2. Generate json_tuple(values#1305, Zipcode, ZipCodeType, City), [id#1304],
    // false, [c0#1407, c1#1408, c2#1409]
    if (exprStr.nonEmpty) {
      val functionPattern = """(\w+)\(.*\)""".r
      val functionName = getFunctionName(functionPattern, exprStr)
      functionName match {
        case Some(func) => parsedExpressions += func
        case _ => // NO OP
      }
    }
    parsedExpressions.distinct.toArray
  }

   // This parser is used for BroadcastHashJoin, ShuffledHashJoin and SortMergeJoin
   def parseEquijoinsExpressions(exprStr: String): (Array[String], Boolean) = {
     // ShuffledHashJoin [name#11, CEIL(DEPT#12)], [name#28, CEIL(DEPT_ID#27)], Inner, BuildLeft
     // SortMergeJoin [name#11, CEIL(dept#12)], [name#28, CEIL(dept_id#27)], Inner
     // BroadcastHashJoin [name#11, CEIL(dept#12)], [name#28, CEIL(dept_id#27)], Inner,
     // BuildRight, false
     val parsedExpressions = ArrayBuffer[String]()
     val pattern = """\[([\w#, +*\\\-\.<>=\`\(\)]+\])""".r
     // Get all the join expressions and split it with delimiter :: so that it could be used to
     // parse function names (if present) later.
     val joinExprs = pattern.findAllMatchIn(exprStr).mkString("::")
     // Get joinType and buildSide(if applicable)
     val joinParams = pattern.replaceAllIn(exprStr, "").split(",").map(_.trim).filter(_.nonEmpty)
     val joinType = joinParams(0).trim
     // SortMergeJoin doesn't have buildSide, assign empty string in that case
     val buildSide = if (joinParams.size > 1) {
       joinParams(1).trim
     } else {
       ""
     }
     // Get individual expressions which is later used to get the function names.
     val expressions = joinExprs.split("::").map(_.trim).map(
       _.replaceAll("""^\[+|\]+$""", "")
       map(_.split(",")).flatten.map(_.trim)

     if (expressions.nonEmpty) {
       val functionPattern = """(\w+)\(.*\)""".r
       expressions.foreach { expr =>
         val functionName = getFunctionName(functionPattern, expr)
         functionName.foreach(parsedExpressions += _)
       }
     }

     (parsedExpressions.distinct.toArray, equiJoinSupportedTypes(buildSide, joinType))
   }

  def parseNestedLoopJoinExpressions(exprStr: String): (Array[String], Boolean) = {
    // BuildRight, LeftOuter, ((CEIL(cast(id1#1490 as double)) <= cast(id2#1496 as bigint))
    // AND (cast(id1#1490 as bigint) < CEIL(cast(id2#1496 as double))))
    val parsedExpressions = ArrayBuffer[String]()
    // Get joinType and buildSide by splitting the input string.
    val nestedLoopParameters = exprStr.split(",", 3)
    val buildSide = nestedLoopParameters(0).trim
    val joinType = nestedLoopParameters(1).trim

    if (nestedLoopParameters.size > 2) { // condition present on join columns
      val exprSepAND = if (exprStr.contains("AND")) {
        exprStr.split(" AND ").map(_.trim)
      } else {
        Array(exprStr)
      }
      val exprSplit = if (exprStr.contains(" OR ")) {
        exprSepAND.flatMap(_.split(" OR ").map(_.trim))
      } else {
        exprSepAND
      }

      // Remove parentheses from the beginning and end to get only the expressions
      val paranRemoved = exprSplit.map(_.replaceAll("""^\(+""", "").replaceAll("""\)\)$""", ")"))
      val functionPattern = """(\w+)\(.*\)""".r
      val conditionalExprPattern = """([(\w# )]+) ([+=<>|]+) ([(\w# )]+)""".r
      paranRemoved.foreach { case expr =>
        if (expr.contains(" ")) {
          conditionalExprPattern.findFirstMatchIn(expr) match {
            case Some(func) =>
              logDebug(s" found expr: $func")
              if (func.groupCount < 3) {
                logError(s"found incomplete expression - $func")
              }
              // get expressions from condition
              val expressions = Array(func.group(1), func.group(3))
              expressions.foreach { expr =>
                // Add function name to result
                val functionName = getFunctionName(functionPattern, expr)
                functionName match {
                  case Some(func) => parsedExpressions += func
                  case _ => // NO OP
                }
              }
            case None => logDebug(s"Incorrect expression - $expr")
          }
        } else {
          // likely some function call, add function name to result
          val functionName = getFunctionName(functionPattern, expr)
          functionName match {
            case Some(func) => parsedExpressions += func
            case _ => // NO OP
          }
        }
      }
    } else {
      // NO OP
    }
    (parsedExpressions.distinct.toArray, nestedLoopJoinSupportedTypes(buildSide, joinType))
  }

  private def isJoinTypeSupported(joinType: String): Boolean = {
    joinType match {
      case JoinType.Cross => true
      case JoinType.Inner => true
      case JoinType.LeftSemi => true
      case JoinType.LeftOuter => true
      case JoinType.RightOuter => true
      case JoinType.LeftAnti => true
      case _ => false
    }
  }

  private def equiJoinSupportedTypes(buildSide: String, joinType: String): Boolean = {
    val joinTypeSupported = isJoinTypeSupported(joinType)
    // This is from GpuHashJoin.tagJoin where the Exec is tagged to run on GPU if one of the
    // below condition is met.
    val buildSideSupported = if (buildSide == BuildSide.BuildLeft) {
      joinType == JoinType.Inner || joinType == JoinType.Cross ||
        joinType == JoinType.RightOuter || joinType == JoinType.FullOuter
    } else if (buildSide == BuildSide.BuildRight) {
      joinType == JoinType.Inner || joinType == JoinType.Cross ||
        joinType == JoinType.LeftOuter || joinType == JoinType.LeftSemi ||
        joinType == JoinType.LeftAnti
    } else {
      true
    }
    joinTypeSupported && buildSideSupported
  }

  private def nestedLoopJoinSupportedTypes(buildSide: String, joinType: String): Boolean = {
    val joinTypeSupported = isJoinTypeSupported(joinType)

    // This is from GpuBroadcastNestedLoopJoinMeta.tagPlanForGpu where join is
    // not supported on GPU if below condition is met.
    val buildSideNotSupported = if (buildSide == BuildSide.BuildLeft) {
      joinType == JoinType.LeftOuter || joinType == JoinType.LeftSemi ||
        joinType == JoinType.LeftAnti
    } else if (buildSide == BuildSide.BuildRight) {
      joinType == JoinType.RightOuter
    } else {
      false
    }
    joinTypeSupported && !buildSideNotSupported
  }

  def parseSortExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()
    // Sort [round(num#126, 0) ASC NULLS FIRST, letter#127 DESC NULLS LAST], true, 0
    val pattern = """\[([\w#, \(\)]+\])""".r
    val sortString = pattern.findFirstMatchIn(exprStr)
    // This is to split multiple column names in SortExec. Project may have a function on a column.
    // The string is split on delimiter containing FIRST, OR LAST, which is the last string
    // of each column in SortExec that produces an array containing
    // column names. Finally we remove the parentheses from the beginning and end to get only
    // the expressions. Result will be as below.
    // paranRemoved = Array(round(num#7, 0) ASC NULLS FIRST,, letter#8 DESC NULLS LAST)
    if (sortString.isDefined) {
      val paranRemoved = sortString.get.toString.split("(?<=FIRST,)|(?<=LAST,)").
          map(_.trim).map(_.replaceAll("""^\[+""", "").replaceAll("""\]+$""", ""))
      val functionPattern = """(\w+)\(.*\)""".r
      paranRemoved.foreach { case expr =>
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }

  def parseFilterExpressions(exprStr: String): Array[String] = {
    val parsedExpressions = ArrayBuffer[String]()

    // Filter ((isnotnull(s_state#68) AND (s_state#68 = TN)) OR (hex(cast(value#0 as bigint)) = B))
    // split on AND/OR/NOT
    val exprSepAND = if (exprStr.contains("AND")) {
      exprStr.split(" AND ").map(_.trim)
    } else {
      Array(exprStr)
    }
    val exprSepOR = if (exprStr.contains(" OR ")) {
      exprSepAND.flatMap(_.split(" OR ").map(_.trim))
    } else {
      exprSepAND
    }

    val exprSplit = if (exprStr.contains("NOT ")) {
      parsedExpressions += "Not"
      exprSepOR.flatMap(_.split("NOT ").map(_.trim))
    } else {
      exprSepOR
    }

    // Remove paranthesis from the beginning and end to get only the expressions
    val paranRemoved = exprSplit.map(_.replaceAll("""^\(+""", "").replaceAll("""\)\)$""", ")"))
    val functionPattern = """(\w+)\(.*\)""".r
    val conditionalExprPattern = """([(\w# )]+) ([+=<>|]+) ([(\w# )]+)""".r

    paranRemoved.foreach { case expr =>
      if (expr.contains(" ")) {
        // likely some conditional expression
        // TODO - add in arithmetic stuff (- / * )
        conditionalExprPattern.findFirstMatchIn(expr) match {
          case Some(func) =>
            logDebug(s" found expr: $func")
            if (func.groupCount < 3) {
              logError(s"found incomplete expression - $func")
            }
            val lhs = func.group(1)
            // Add function name to result
            val functionName = getFunctionName(functionPattern, lhs)
             functionName match {
               case Some(func) => parsedExpressions += func
               case _ => // NO OP
             }
            val predicate = func.group(2)
            val rhs = func.group(3)
            // check for variable
            if (lhs.contains("#") || rhs.contains("#")) {
              logDebug(s"expr contains # $lhs or $rhs")
            }
            val predStr = predicate match {
              case "=" => "EqualTo"
              case "<=>" => "EqualNullSafe"
              case "<" => "LessThan"
              case ">" => "GreaterThan"
              case "<=" => "LessThanOrEqual"
              case ">=" => "GreaterThanOrEqual"
              case "+" => "Add"
            }
            logDebug(s"predicate string is $predStr")
            parsedExpressions += predStr
          // TODO - lookup function name
          case None => logDebug(s"Incorrect expression - $expr")
        }
      } else {
        // likely some function call, add function name to result
        val functionName = getFunctionName(functionPattern, expr)
        functionName match {
          case Some(func) => parsedExpressions += func
          case _ => // NO OP
        }
      }
    }
    parsedExpressions.distinct.toArray
  }
}
