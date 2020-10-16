/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.util.QueryExecutionListener

case class ColumnarOverrideRules() extends ColumnarRule with Logging {
  val overrides: Rule[SparkPlan] = GpuOverrides()
  val overrideTransitions: Rule[SparkPlan] = new GpuTransitionOverrides()

  override def preColumnarTransitions : Rule[SparkPlan] = overrides

  override def postColumnarTransitions: Rule[SparkPlan] = overrideTransitions
}

/**
 * Extension point to enable GPU SQL processing.
 */
class SQLExecPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing extensions to enable rapids GPU SQL support." +
      s" To disable GPU support set `${RapidsConf.SQL_ENABLED}` to false")
    extensions.injectColumnar(_ => ColumnarOverrideRules())
    ShimLoader.getSparkShims.injectQueryStagePrepRule(extensions, _ => GpuQueryStagePrepOverrides())
  }
}

object RapidsPluginUtils extends Logging {
  private val SQL_PLUGIN_NAME = classOf[SQLExecPlugin].getName
  private val SQL_PLUGIN_CONF_KEY = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  private val SERIALIZER_CONF_KEY = "spark.serializer"
  private val JAVA_SERIALIZER_NAME = classOf[JavaSerializer].getName
  private val KRYO_SERIALIZER_NAME = classOf[KryoSerializer].getName
  private val KRYO_REGISRATOR_KEY = "spark.kryo.registrator"
  private val KRYO_REGISRATOR_NAME = classOf[GpuKryoRegistrator].getName

  def fixupConfigs(conf: SparkConf): Unit = {
    // First add in the SQL executor plugin because that is what we need at a minimum
    if (conf.contains(SQL_PLUGIN_CONF_KEY)) {
      val previousValue = conf.get(SQL_PLUGIN_CONF_KEY).split(",").map(_.trim)
      if (!previousValue.contains(SQL_PLUGIN_NAME)) {
        conf.set(SQL_PLUGIN_CONF_KEY, (previousValue :+ SQL_PLUGIN_NAME).mkString(","))
      } else {
        conf.set(SQL_PLUGIN_CONF_KEY, previousValue.mkString(","))
      }
    } else {
      conf.set(SQL_PLUGIN_CONF_KEY, SQL_PLUGIN_NAME)
    }

    val serializer = conf.get(SERIALIZER_CONF_KEY, JAVA_SERIALIZER_NAME)
    if (KRYO_SERIALIZER_NAME.equals(serializer)) {
      if (conf.contains(KRYO_REGISRATOR_KEY)) {
        if (!KRYO_REGISRATOR_NAME.equals(conf.get(KRYO_REGISRATOR_KEY)) ) {
          logWarning("Rapids SQL Plugin when used with Kryo needs to register some " +
            s"serializers using $KRYO_REGISRATOR_NAME. Please call it from your registrator " +
            " to let the plugin work properly.")
        } // else it is set and we are good to go
      }  else {
        // We cannot set the kryo key here, it is not early enough to be picked up everywhere
        throw new UnsupportedOperationException("The Rapids SQL Plugin when used with Kryo needs " +
          s"to register some serializers. Please set the spark config $KRYO_REGISRATOR_KEY to " +
          s"$KRYO_REGISRATOR_NAME or some operations may not work properly.")
      }
    } else if (!JAVA_SERIALIZER_NAME.equals(serializer)) {
      throw new UnsupportedOperationException(s"$serializer is not a supported serializer for " +
        s"the Rapids SQL Plugin. Please disable the rapids plugin or use a supported serializer " +
        s"serializer ($JAVA_SERIALIZER_NAME, $KRYO_SERIALIZER_NAME).")
    }
  }
}

/**
 * The Spark driver plugin provided by the RAPIDS Spark plugin.
 */
class RapidsDriverPlugin extends DriverPlugin with Logging {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val sparkConf = pluginContext.conf
    RapidsPluginUtils.fixupConfigs(sparkConf)
    val conf = new RapidsConf(sparkConf)
    if (conf.shimsProviderOverride.isDefined) {
      ShimLoader.setSparkShimProviderClass(conf.shimsProviderOverride.get)
    }
    conf.rapidsConfMap
  }
}

/**
 * The Spark executor plugin provided by the RAPIDS Spark plugin.
 */
class RapidsExecutorPlugin extends ExecutorPlugin with Logging {
  override def init(
      pluginContext: PluginContext,
      extraConf: util.Map[String, String]): Unit = {
    try {
      val conf = new RapidsConf(extraConf.asScala.toMap)
      if (conf.shimsProviderOverride.isDefined) {
        ShimLoader.setSparkShimProviderClass(conf.shimsProviderOverride.get)
      }

      // we rely on the Rapids Plugin being run with 1 GPU per executor so we can initialize
      // on executor startup.
      if (!GpuDeviceManager.rmmTaskInitEnabled) {
        logInfo("Initializing memory from Executor Plugin")
        GpuDeviceManager.initializeGpuAndMemory(pluginContext.resources().asScala.toMap)
      }

      GpuSemaphore.initialize(conf.concurrentGpuTasks)
    } catch {
      case e: Throwable =>
        // Exceptions in executor plugin can cause a single thread to die but the executor process
        // sticks around without any useful info until it hearbeat times out. Print what happened
        // and exit immediately.
        logError("Exception in the executor plugin", e)
        System.exit(1)
    }
  }

  override def shutdown(): Unit = {
    GpuSemaphore.shutdown()
    PythonWorkerSemaphore.shutdown()
  }
}

object ExecutionPlanCaptureCallback {
  private[this] val shouldCapture: AtomicBoolean = new AtomicBoolean(false)
  private[this] val execPlan: AtomicReference[SparkPlan] = new AtomicReference[SparkPlan]()

  private def captureIfNeeded(qe: QueryExecution): Unit = {
    if (shouldCapture.get()) {
      execPlan.set(qe.executedPlan)
    }
  }

  def startCapture(): Unit = {
    execPlan.set(null)
    shouldCapture.set(true)
  }

  def getResultWithTimeout(timeoutMs: Long = 2000): Option[SparkPlan] = {
    try {
      val endTime = System.currentTimeMillis() + timeoutMs
      var plan = execPlan.getAndSet(null)
      while (plan == null) {
        if (System.currentTimeMillis() > endTime) {
          return None
        }
        Thread.sleep(10)
        plan = execPlan.getAndSet(null)
      }
      Some(plan)
    } finally {
      shouldCapture.set(false)
      execPlan.set(null)
    }
  }

  def extractExecutedPlan(plan: Option[SparkPlan]): SparkPlan = {
    plan match {
      case Some(p: AdaptiveSparkPlanExec) => p.executedPlan
      case Some(p) => p
      case _ => throw new IllegalStateException("No execution plan available")
    }
  }

  def assertCapturedAndGpuFellBack(fallbackCpuClass: String, timeoutMs: Long = 2000): Unit = {
    val gpuPlan = getResultWithTimeout(timeoutMs=timeoutMs)
    assert(gpuPlan.isDefined, "Did not capture a GPU plan")
    assertDidFallBack(gpuPlan.get, fallbackCpuClass)
  }

  def assertDidFallBack(gpuPlan: SparkPlan, fallbackCpuClass: String): Unit = {
    val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(Some(gpuPlan))
    assert(executedPlan.find(didFallBack(_, fallbackCpuClass)).isDefined,
        s"Could not find $fallbackCpuClass in the GPU plan\n$executedPlan")
  }

  private def getBaseNameFromClass(planClassStr: String): String = {
    val firstDotIndex = planClassStr.lastIndexOf(".")
    if (firstDotIndex != -1) planClassStr.substring(firstDotIndex + 1) else planClassStr
  }

  private def didFallBack(exp: Expression, fallbackCpuClass: String): Boolean = {
    if (!exp.isInstanceOf[GpuExpression] &&
      getBaseNameFromClass(exp.getClass.getName) == fallbackCpuClass) {
      true
    } else {
      exp.children.exists(didFallBack(_, fallbackCpuClass))
    }
  }

  private def didFallBack(plan: SparkPlan, fallbackCpuClass: String): Boolean = {
    val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(Some(plan))
    if (!executedPlan.isInstanceOf[GpuExec] &&
      getBaseNameFromClass(executedPlan.getClass.getName) == fallbackCpuClass) {
      true
    } else {
      executedPlan.expressions.exists(didFallBack(_, fallbackCpuClass))
    }
  }
}

/**
 * Used as a part of testing to capture the executed query plan.
 */
class ExecutionPlanCaptureCallback extends QueryExecutionListener {
  import ExecutionPlanCaptureCallback._

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    captureIfNeeded(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    captureIfNeeded(qe)
}
