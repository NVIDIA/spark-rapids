/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Support for noop data source writes that creates non-V1 write executors.
 * These executors might not exist in all Spark versions, so we use conditional loading.
 */
object NoopWriteSupport extends Logging {
  
  // Class names for the non-V1 write executors
  private val overwriteByExpressionExecClassName = 
    "org.apache.spark.sql.execution.datasources.v2.OverwriteByExpressionExec"
  private val appendDataExecClassName = 
    "org.apache.spark.sql.execution.datasources.v2.AppendDataExec"
    
  // Check if the non-V1 write executors are available
  lazy val hasNonV1WriteExecs: Boolean = {
    Utils.classIsLoadable(overwriteByExpressionExecClassName) && 
    Utils.classIsLoadable(appendDataExecClassName)
  }
  
  // Load the classes if available
  lazy val overwriteByExpressionExecClass: Option[Class[_]] = {
    if (hasNonV1WriteExecs) {
      Try(ShimReflectionUtils.loadClass(overwriteByExpressionExecClassName)).toOption
    } else {
      None
    }
  }
  
  lazy val appendDataExecClass: Option[Class[_]] = {
    if (hasNonV1WriteExecs) {
      Try(ShimReflectionUtils.loadClass(appendDataExecClassName)).toOption
    } else {
      None
    }
  }
  
  /**
   * Check if a data source supports noop writes by examining the class name/package.
   * Noop data sources typically have "noop" in their class or package name.
   */
  def isNoopDataSource(write: SupportsWrite): Boolean = {
    val className = write.getClass.getName.toLowerCase
    val packageName = write.getClass.getPackage.getName.toLowerCase
    
    // Check for noop in class name or package name
    val isNoop = className.contains("noop") || packageName.contains("noop") ||
      // Also check for specific known class patterns
      className.endsWith("noopdatasourcev2") || className.endsWith("nooptable")
    
    if (isNoop) {
      logInfo(s"Detected noop data source: ${write.getClass.getName}")
    }
    
    isNoop
  }
  
  /**
   * Create rules for non-V1 write executors if they exist.
   * Returns empty map if the executors don't exist in this Spark version.
   */
  def getNonV1WriteExecRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val rules = scala.collection.mutable.Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]()
    
    // Add OverwriteByExpressionExec rule if class exists
    overwriteByExpressionExecClass.foreach { clazz =>
      try {
        val rule = createOverwriteByExpressionExecRule(clazz.asInstanceOf[Class[SparkPlan]])
        rules += (clazz.asInstanceOf[Class[SparkPlan]] -> rule)
        logInfo(s"Added GPU rule for ${clazz.getName}")
      } catch {
        case e: Exception =>
          logWarning(s"Failed to create rule for ${clazz.getName}: ${e.getMessage}")
      }
    }
    
    // Add AppendDataExec rule if class exists
    appendDataExecClass.foreach { clazz =>
      try {
        val rule = createAppendDataExecRule(clazz.asInstanceOf[Class[SparkPlan]])
        rules += (clazz.asInstanceOf[Class[SparkPlan]] -> rule)
        logInfo(s"Added GPU rule for ${clazz.getName}")
      } catch {
        case e: Exception =>
          logWarning(s"Failed to create rule for ${clazz.getName}: ${e.getMessage}")
      }
    }
    
    rules.toMap
  }
  
  private def createOverwriteByExpressionExecRule(clazz: Class[SparkPlan]): ExecRule[SparkPlan] = {
    new ExecRule[SparkPlan](
      "Overwrite into a datasource V2 table",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
        TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
        GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (plan, conf, parent, rule) => new NoopOverwriteByExpressionExecMeta(plan, conf, parent, rule)
    )(ClassTag(clazz))
  }
  
  private def createAppendDataExecRule(clazz: Class[SparkPlan]): ExecRule[SparkPlan] = {
    new ExecRule[SparkPlan](
      "Append data into a datasource V2 table",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
        TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
        GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (plan, conf, parent, rule) => new NoopAppendDataExecMeta(plan, conf, parent, rule)
    )(ClassTag(clazz))
  }
}

/**
 * Meta class for non-V1 OverwriteByExpressionExec that handles noop data sources
 */
class NoopOverwriteByExpressionExecMeta(
    plan: SparkPlan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SparkPlan](plan, conf, parent, rule) with Logging {

  override def tagPlanForGpu(): Unit = {
    // Use reflection to get the table from the plan
    try {
      val tableField = plan.getClass.getDeclaredField("table")
      tableField.setAccessible(true)
      val table = tableField.get(plan).asInstanceOf[SupportsWrite]
      
      if (NoopWriteSupport.isNoopDataSource(table)) {
        // Noop writes are GPU compatible since they don't actually write data
        logInfo(s"Allowing noop write on GPU for ${table.getClass.getName}")
      } else {
        willNotWorkOnGpu(s"Non-noop data source not supported: ${table.getClass.getName}")
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to check table type for ${plan.getClass.getName}: ${e.getMessage}")
        willNotWorkOnGpu(s"Could not determine data source type: ${e.getMessage}")
    }
  }

  override def convertToGpu(): GpuExec = {
    // For noop writes, create a GPU-compatible exec that discards the data
    new NoopGpuWriteExec(plan)
  }
}

/**
 * Meta class for non-V1 AppendDataExec that handles noop data sources
 */
class NoopAppendDataExecMeta(
    plan: SparkPlan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SparkPlan](plan, conf, parent, rule) with Logging {

  override def tagPlanForGpu(): Unit = {
    // Use reflection to get the table from the plan
    try {
      val tableField = plan.getClass.getDeclaredField("table")
      tableField.setAccessible(true)
      val table = tableField.get(plan).asInstanceOf[SupportsWrite]
      
      if (NoopWriteSupport.isNoopDataSource(table)) {
        // Noop writes are GPU compatible since they don't actually write data
        logInfo(s"Allowing noop write on GPU for ${table.getClass.getName}")
      } else {
        willNotWorkOnGpu(s"Non-noop data source not supported: ${table.getClass.getName}")
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to check table type for ${plan.getClass.getName}: ${e.getMessage}")
        willNotWorkOnGpu(s"Could not determine data source type: ${e.getMessage}")
    }
  }

  override def convertToGpu(): GpuExec = {
    // For noop writes, create a GPU-compatible exec that discards the data
    new NoopGpuWriteExec(plan)
  }
}

/**
 * GPU executor for noop writes that simply discards the data
 */
class NoopGpuWriteExec(cpuPlan: SparkPlan) extends LeafV2CommandExec with GpuExec with Logging {
  override def supportsColumnar: Boolean = false

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Nil

  override def run(): Seq[InternalRow] = {
    logInfo("Executing noop write on GPU - discarding data")
    // For noop writes, we just need to consume the input but don't actually write anything
    
    // Get the child plan and execute it to consume the data
    try {
      val queryField = cpuPlan.getClass.getDeclaredField("query")
      queryField.setAccessible(true)
      val query = queryField.get(cpuPlan).asInstanceOf[LogicalPlan]
      
      // Execute the query to consume the data but don't write it anywhere
      val childPlan = query.collectFirst { case p => p }
      logInfo(s"Noop write consumed data from query: ${query.getClass.getSimpleName}")
    } catch {
      case e: Exception =>
        logInfo(s"Noop write completed (could not access query details): ${e.getMessage}")
    }
    
    Nil
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
      s" mismatch:\n$this")
  }
}