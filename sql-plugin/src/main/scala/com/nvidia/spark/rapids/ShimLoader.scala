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

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

import com.nvidia.spark.rapids._
import org.apache.spark.sql.rapids.execution._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.internal.Logging

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}
import org.apache.spark.internal.Logging

object ShimLoader extends Logging {

  val SPARK30DATABRICKSSVERSIONNAME = "3.0.0-databricks"
  val SPARK30VERSIONNAME = "3.0.0"
  val SPARK31VERSIONNAME = "3.1.0-SNAPSHOT"

  private var sparkShims: SparkShims = null
  private var gpuBroadcastNestedJoinShims: GpuBroadcastNestedLoopJoinExecBase = null

  /**
   * The names of the classes for shimming Spark for each major version.
   */
  private val SPARK_SHIM_CLASSES = HashMap(
    SPARK30VERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark30Shims",
    SPARK30DATABRICKSSVERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark300DatabricksShims",
    SPARK31VERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark31Shims",
  )

  /**
   * Factory method to get an instance of HadoopShims based on the
   * version of Hadoop on the classpath.
   */
  def getSparkShims: SparkShims = {
    if (sparkShims == null) {
      sparkShims = loadShims(SPARK_SHIM_CLASSES, classOf[SparkShims])
    }
    sparkShims
  }

  private val BROADCAST_NESTED_LOOP_JOIN_SHIM_CLASSES = HashMap(
    SPARK30VERSIONNAME -> "com.nvidia.spark.rapids.shims.GpuBroadcastNestedLoopJoinExec30",
    SPARK30DATABRICKSSVERSIONNAME -> "com.nvidia.spark.rapids.shims.GpuBroadcastNestedLoopJoinExec300Databricks",
    SPARK31VERSIONNAME -> "com.nvidia.spark.rapids.shims.GpuBroadcastNestedLoopJoinExec31",
  )

  def getGpuBroadcastNestedLoopJoinShims(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression]): GpuBroadcastNestedLoopJoinExecBase = {
    if (sparkShims == null) {
      gpuBroadcastNestedJoinShims = loadShimsNestedBroadcastJoin(BROADCAST_NESTED_LOOP_JOIN_SHIM_CLASSES, classOf[GpuBroadcastNestedLoopJoinExecBase],
        left, right, join, joinType, condition)
    }
    gpuBroadcastNestedJoinShims
  }

  private def loadShims[T](classMap: Map[String, String], xface: Class[T]): T = {
    val vers = getVersion();
    val className = classMap.get(vers)
    if (className.isEmpty) {
      throw new Exception(s"No shim layer for $vers")
    } 
    createShim(className.get, xface)
  }

  private def createShim[T](className: String, xface: Class[T]): T = try {
    val clazz = Class.forName(className)
    val res = clazz.newInstance().asInstanceOf[T]
    res
  } catch {
    case e: Exception => throw new RuntimeException("Could not load shims in class " + className, e)
  }

  private def loadShimsNestedBroadcastJoin[T](classMap: Map[String, String], xface: Class[T],
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression]): T = {
    val vers = getVersion();
    val className = classMap.get(vers)
    if (className.isEmpty) {
      throw new Exception(s"No shim layer for $vers")
    } 
    createShimNestedBroadcastJoin(className.get, xface, left, right, join, joinType, condition)
  }

  private def createShimNestedBroadcastJoin[T](className: String, xface: Class[T],
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression]): T = try {
    val clazz = Class.forName(className)
    val resultMethod = clazz.getDeclaredMethod("createInstance", classOf[org.apache.spark.sql.execution.SparkPlan],classOf[org.apache.spark.sql.execution.SparkPlan],classOf[org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec], classOf[org.apache.spark.sql.catalyst.plans.JoinType], classOf[scala.Option[org.apache.spark.sql.catalyst.expressions.Expression]])
    val res = resultMethod.invoke(clazz, left, right, join, joinType, condition).asInstanceOf[T]
    res
  } catch {
    case e: Exception => throw new RuntimeException("Could not load shims in class " + className, e)
  }

  def getVersion(): String = {
    // hack for databricks, try to find something more reliable?
    if (SPARK_BUILD_USER.equals("Databricks")) {
        SPARK_VERSION + "-databricks"
    } else {
      SPARK_VERSION
    }
  }

}
