/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters._
import scala.util.Try

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
import org.apache.spark.sql.rapids.GpuShuffleEnv
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
  private val UDF_PLUGIN_NAME = "com.nvidia.spark.udf.Plugin"
  private val SQL_PLUGIN_CONF_KEY = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  private val SERIALIZER_CONF_KEY = "spark.serializer"
  private val JAVA_SERIALIZER_NAME = classOf[JavaSerializer].getName
  private val KRYO_SERIALIZER_NAME = classOf[KryoSerializer].getName
  private val KRYO_REGISRATOR_KEY = "spark.kryo.registrator"
  private val KRYO_REGISRATOR_NAME = classOf[GpuKryoRegistrator].getName

  def fixupConfigs(conf: SparkConf): Unit = {
    // First add in the SQL executor plugin because that is what we need at a minimum
    if (conf.contains(SQL_PLUGIN_CONF_KEY)) {
      for (pluginName <- Array(SQL_PLUGIN_NAME, UDF_PLUGIN_NAME)){
        val previousValue = conf.get(SQL_PLUGIN_CONF_KEY).split(",").map(_.trim)
        if (!previousValue.contains(pluginName)) {
          conf.set(SQL_PLUGIN_CONF_KEY, (previousValue :+ pluginName).mkString(","))
        } else {
          conf.set(SQL_PLUGIN_CONF_KEY, previousValue.mkString(","))
        }
      }
    } else {
      conf.set(SQL_PLUGIN_CONF_KEY, Array(SQL_PLUGIN_NAME,UDF_PLUGIN_NAME).mkString(","))
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
  var rapidsShuffleHeartbeatManager: RapidsShuffleHeartbeatManager = null

  override def receive(msg: Any): AnyRef = {
    if (rapidsShuffleHeartbeatManager == null) {
      throw new IllegalStateException(
        s"Rpc message $msg received, but shuffle heartbeat manager not configured.")
    }
    msg match {
      case RapidsExecutorStartupMsg(id) =>
        rapidsShuffleHeartbeatManager.registerExecutor(id)
      case RapidsExecutorHeartbeatMsg(id) =>
        rapidsShuffleHeartbeatManager.executorHeartbeat(id)
      case m => throw new IllegalStateException(s"Unknown message $m")
    }
  }

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val sparkConf = pluginContext.conf
    RapidsPluginUtils.fixupConfigs(sparkConf)
    val conf = new RapidsConf(sparkConf)
    if (conf.shimsProviderOverride.isDefined) {
      ShimLoader.setSparkShimProviderClass(conf.shimsProviderOverride.get)
    }
    if (GpuShuffleEnv.isRapidsShuffleEnabled &&
        conf.shuffleTransportEarlyStart) {
      rapidsShuffleHeartbeatManager = new RapidsShuffleHeartbeatManager()
    }
    conf.rapidsConfMap
  }
}

/**
 * The Spark executor plugin provided by the RAPIDS Spark plugin.
 */
class RapidsExecutorPlugin extends ExecutorPlugin with Logging {
  var rapidsShuffleHeartbeatEndpoint: RapidsShuffleHeartbeatEndpoint = null

  override def init(
      pluginContext: PluginContext,
      extraConf: util.Map[String, String]): Unit = {
    try {
      val conf = new RapidsConf(extraConf.asScala.toMap)
      if (conf.shimsProviderOverride.isDefined) {
        ShimLoader.setSparkShimProviderClass(conf.shimsProviderOverride.get)
      }

      // Compare if the cudf version mentioned in the classpath is equal to the version which
      // plugin expects. If there is a version mismatch, throw error. This check can be disabled
      // by setting this config spark.rapids.cudfVersionOverride=true
      checkCudfVersion(conf)

      // we rely on the Rapids Plugin being run with 1 GPU per executor so we can initialize
      // on executor startup.
      if (!GpuDeviceManager.rmmTaskInitEnabled) {
        logInfo("Initializing memory from Executor Plugin")
        GpuDeviceManager.initializeGpuAndMemory(pluginContext.resources().asScala.toMap)
        if (GpuShuffleEnv.isRapidsShuffleEnabled &&
            conf.shuffleTransportEarlyStart) {
          logInfo("Initializing shuffle manager heartbeats")
          rapidsShuffleHeartbeatEndpoint = new RapidsShuffleHeartbeatEndpoint(pluginContext, conf)
        }
      }

      val concurrentGpuTasks = conf.concurrentGpuTasks
      logInfo(s"The number of concurrent GPU tasks allowed is $concurrentGpuTasks")
      GpuSemaphore.initialize(concurrentGpuTasks)
    } catch {
      case e: Throwable =>
        // Exceptions in executor plugin can cause a single thread to die but the executor process
        // sticks around without any useful info until it hearbeat times out. Print what happened
        // and exit immediately.
        logError("Exception in the executor plugin", e)
        System.exit(1)
    }
  }

  private def checkCudfVersion(conf: RapidsConf): Unit = {
    try {
      val cudfPropertiesFileName = "cudf-java-version-info.properties"
      val pluginPropertiesFileName = "rapids4spark-version-info.properties"

      val props = new Properties
      val classLoader = classOf[RapidsExecutorPlugin].getClassLoader
      val cudfProperties = classLoader.getResourceAsStream(cudfPropertiesFileName)
      if (cudfProperties == null) {
        throw CudfVersionMismatchException(s"Could not find properties file " +
          s"$cudfPropertiesFileName in the cudf jar. Cannot verify cudf version compatibility " +
          s"with RAPIDS Accelerator version.")
      }
      props.load(cudfProperties)

      val classpathCudfVersion = props.get("version")
      if (classpathCudfVersion == null) {
        throw CudfVersionMismatchException(s"Property name `version` not found in " +
          s"$cudfPropertiesFileName file.")
      }
      val cudfVersion = classpathCudfVersion.toString

      val pluginResource = classLoader.getResourceAsStream(pluginPropertiesFileName)
      if (pluginResource == null) {
        throw CudfVersionMismatchException(s"Could not find properties file " +
          s"$pluginPropertiesFileName in the RAPIDS Accelerator jar. Cannot verify cudf " +
          s"version compatibility with RAPIDS Accelerator version.")
      }
      props.load(pluginResource)

      val pluginCudfVersion = props.get("cudf_version")
      if (pluginCudfVersion == null) {
        throw CudfVersionMismatchException(s"Property name `cudf_version` not found in" +
          s" $pluginPropertiesFileName file.")
      }
      val expectedCudfVersion = pluginCudfVersion.toString
      // compare cudf version in the classpath with the cudf version expected by plugin
      if (!RapidsExecutorPlugin.cudfVersionSatisfied(expectedCudfVersion, cudfVersion)) {
        throw CudfVersionMismatchException(s"Cudf version in the classpath is different. " +
          s"Found $cudfVersion, RAPIDS Accelerator expects $expectedCudfVersion")
      }
    } catch {
      case x: CudfVersionMismatchException if conf.cudfVersionOverride =>
        logWarning(s"${x.errorMsg}")
    }
  }

  case class CudfVersionMismatchException(errorMsg: String) extends RuntimeException(errorMsg)

  override def shutdown(): Unit = {
    GpuSemaphore.shutdown()
    PythonWorkerSemaphore.shutdown()
    GpuDeviceManager.shutdown()
    Option(rapidsShuffleHeartbeatEndpoint).foreach(_.close())
  }
}

object RapidsExecutorPlugin {
  /**
   * Return true if the expected cudf version is satisfied by the actual version found.
   * The version is satisfied if the major and minor versions match exactly. If there is a requested
   * patch version then the actual patch version must be greater than or equal.
   * For example, version 7.1 is not satisfied by version 7.2, but version 7.1 is satisfied by
   * version 7.1.1.
   */
  def cudfVersionSatisfied(expected: String, actual: String): Boolean = {
    val (expMajorMinor, expPatch) = expected.split('.').splitAt(2)
    val (actMajorMinor, actPatch) = actual.split('.').splitAt(2)
    actMajorMinor.startsWith(expMajorMinor) && {
      val expPatchInts = expPatch.map(_.toInt)
      val actPatchInts = actPatch.map(v => Try(v.toInt).getOrElse(Int.MinValue))
      val zipped = expPatchInts.zipAll(actPatchInts, 0, 0)
      zipped.forall { case (e, a) => e <= a }
    }
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
