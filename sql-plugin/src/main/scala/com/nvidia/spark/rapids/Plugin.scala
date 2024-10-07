/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import java.lang.reflect.InvocationTargetException
import java.net.URL
import java.time.ZoneId
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.Try

import ai.rapids.cudf.{Cuda, CudaException, CudaFatalException, CudfException, MemoryCleaner, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsConf.AllowMultipleJars
import com.nvidia.spark.rapids.RapidsPluginUtils.buildInfoEvent
import com.nvidia.spark.rapids.filecache.{FileCache, FileCacheLocalityManager, FileCacheLocalityMsg}
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.{ExceptionFailure, SparkConf, SparkContext, TaskContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class PluginException(msg: String) extends RuntimeException(msg)

case class CudfVersionMismatchException(errorMsg: String) extends PluginException(errorMsg)

case class ColumnarOverrideRules() extends ColumnarRule with Logging {
  lazy val overrides: Rule[SparkPlan] = GpuOverrides()
  lazy val overrideTransitions: Rule[SparkPlan] = new GpuTransitionOverrides()

  override def preColumnarTransitions : Rule[SparkPlan] = overrides

  override def postColumnarTransitions: Rule[SparkPlan] = overrideTransitions
}

object RapidsPluginUtils extends Logging {
  val CUDF_PROPS_FILENAME = "cudf-java-version-info.properties"
  val JNI_PROPS_FILENAME = "spark-rapids-jni-version-info.properties"
  val PLUGIN_PROPS_FILENAME = "rapids4spark-version-info.properties"
  private val PRIVATE_PROPS_FILENAME = "rapids4spark-private-version-info.properties"

  private val SQL_PLUGIN_NAME = classOf[SQLExecPlugin].getName
  private val UDF_PLUGIN_NAME = "com.nvidia.spark.udf.Plugin"
  private val SQL_PLUGIN_CONF_KEY = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  private val SERIALIZER_CONF_KEY = "spark.serializer"
  private val JAVA_SERIALIZER_NAME = classOf[JavaSerializer].getName
  private val KRYO_SERIALIZER_NAME = classOf[KryoSerializer].getName
  private val KRYO_REGISTRATOR_KEY = "spark.kryo.registrator"
  private val KRYO_REGISTRATOR_NAME = classOf[GpuKryoRegistrator].getName
  private val EXECUTOR_CORES_KEY = "spark.executor.cores"
  private val TASK_GPU_AMOUNT_KEY = "spark.task.resource.gpu.amount"
  private val EXECUTOR_GPU_AMOUNT_KEY = "spark.executor.resource.gpu.amount"
  private val SPARK_MASTER = "spark.master"
  private val SPARK_RAPIDS_REPO_URL = "https://github.com/NVIDIA/spark-rapids"

  lazy val buildInfoEvent = SparkRapidsBuildInfoEvent(
    sparkRapidsBuildInfo = loadProps(PLUGIN_PROPS_FILENAME),
    sparkRapidsJniBuildInfo = loadProps(JNI_PROPS_FILENAME),
    cudfBuildInfo = loadProps(CUDF_PROPS_FILENAME),
    sparkRapidsPrivateBuildInfo =loadProps(PRIVATE_PROPS_FILENAME)
  )

  {
    logInfo(s"RAPIDS Accelerator build: ${buildInfoEvent.sparkRapidsBuildInfo}")
    logInfo(s"RAPIDS Accelerator JNI build: ${buildInfoEvent.sparkRapidsJniBuildInfo}")
    logInfo(s"cudf build: ${buildInfoEvent.cudfBuildInfo}")
    logInfo(s"RAPIDS Accelerator Private ${buildInfoEvent.sparkRapidsPrivateBuildInfo}")
    val pluginVersion = buildInfoEvent.sparkRapidsBuildInfo.getOrElse("version", "UNKNOWN")
    val cudfVersion = buildInfoEvent.cudfBuildInfo.getOrElse("version", "UNKNOWN")
    val privateRev = buildInfoEvent.sparkRapidsPrivateBuildInfo.getOrElse("revision", "UNKNOWN")
    logWarning(s"RAPIDS Accelerator $pluginVersion using cudf ${cudfVersion}, " +
      s"private revision ${privateRev}")
  }

  val extraPlugins = getExtraPlugins

  def logPluginMode(conf: RapidsConf): Unit = {
    if (conf.isSqlEnabled && conf.isSqlExecuteOnGPU) {
      logWarning("RAPIDS Accelerator is enabled, to disable GPU " +
        s"support set `${RapidsConf.SQL_ENABLED}` to false.")

      if (conf.explain != "NONE") {
        logWarning(s"spark.rapids.sql.explain is set to `${conf.explain}`. Set it to 'NONE' to " +
          "suppress the diagnostics logging about the query placement on the GPU.")
      }

    } else if (conf.isSqlEnabled && conf.isSqlExplainOnlyEnabled) {
      logWarning("RAPIDS Accelerator is in explain only mode, to disable " +
        s"set `${RapidsConf.SQL_ENABLED}` to false. To change the mode, " +
        s"restart the application and change `${RapidsConf.SQL_MODE}`.")
    } else {
      logWarning("RAPIDS Accelerator is disabled, to enable GPU " +
        s"support set `${RapidsConf.SQL_ENABLED}` to true.")
    }

    if (conf.isUdfCompilerEnabled) {
      logWarning("Experimental RAPIDS UDF compiler is enabled, in case of related failures " +
      s"disable it by setting `${RapidsConf.UDF_COMPILER_ENABLED}` to false. " +
      "More information is available at " +
      "https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#" +
      "automatic-translation-of-scala-udfs-to-apache-spark-operations" )
    }
  }

  private def detectMultipleJar(propName: String, jarName: String, conf: RapidsConf): Unit = {
    val classloader = ShimLoader.getShimClassLoader()
    val possibleRapidsJarURLs = classloader.getResources(propName).asScala.toSet.toSeq.filter {
      url => {
        val urlPath = url.toString
        // Filter out submodule jars, e.g. rapids-4-spark-aggregator_2.12-24.12.0-spark341.jar,
        // and files stored under subdirs of '!/', e.g.
        // rapids-4-spark_2.12-24.12.0-cuda11.jar!/spark330/rapids4spark-version-info.properties
        // We only want to find the main jar, e.g.
        // rapids-4-spark_2.12-24.12.0-cuda11.jar!/rapids4spark-version-info.properties
        !urlPath.contains("rapids-4-spark-") && urlPath.endsWith("!/" + propName)
      }
    }
    val revisionRegex = "revision=(.*)".r
    val revisionMap: Map[String, Seq[URL]] = possibleRapidsJarURLs.map { url =>
      val versionInfo = scala.io.Source.fromURL(url).getLines().toSeq
      val revision = versionInfo
        .collect {
          case revisionRegex(revision) => revision
        }
        .headOption
        .getOrElse("UNKNOWN")
      (revision, url)
    }.groupBy(_._1).mapValues(_.map(_._2)).toMap
    lazy val rapidsJarsVersMsg = revisionMap.map {
      case (revision, urls) => {
        s"revison: $revision" + urls.map {
          url => "\n\tjar URL: " + url.toString.split("!").head + "\n\t" +
              scala.io.Source.fromURL(url).getLines().toSeq.mkString("\n\t")
        }.mkString + "\n"
      }
    }.mkString
    lazy val msg = s"Multiple $jarName jars found in the classpath:\n$rapidsJarsVersMsg" +
        s"Please make sure there is only one $jarName jar in the classpath. "

    // revisionMap.size could be 0 when debugging in IDE, so allow it in that case
    conf.allowMultipleJars match {
      case AllowMultipleJars.ALWAYS =>
        if (revisionMap.size > 1 || revisionMap.values.exists(_.size != 1)) {
          logWarning(msg)
        }
      case AllowMultipleJars.SAME_REVISION =>
        val recommended = "If it is impossible to fix the classpath you can suppress the " +
              s"error by setting ${RapidsConf.ALLOW_MULTIPLE_JARS.key} to ALWAYS, but this " +
              s"can cause unpredictable behavior as the plugin may pick up the wrong jar."
        require(revisionMap.size <= 1, msg + recommended)
        if (revisionMap.values.exists(_.size != 1)) {
          logWarning(msg + recommended)
        }
      case AllowMultipleJars.NEVER =>
        val recommended = "If it is impossible to fix the classpath you can suppress the " +
            s"error by setting ${RapidsConf.ALLOW_MULTIPLE_JARS.key} to SAME_REVISION or ALWAYS." +
            " But setting it to ALWAYS can cause unpredictable behavior as the plugin may pick " +
            "up the wrong jar."
        require(revisionMap.size <= 1 && revisionMap.values.forall(_.size == 1), msg + recommended)
    }
  }

  def detectMultipleJars(conf: RapidsConf): Unit = {
    detectMultipleJar(PLUGIN_PROPS_FILENAME, "rapids-4-spark", conf)
    detectMultipleJar(JNI_PROPS_FILENAME, "spark-rapids-jni", conf)
    detectMultipleJar(CUDF_PROPS_FILENAME, "cudf", conf)
  }

  // This assumes Apache Spark logic, if CSPs are setting defaults differently, we may need
  // to handle.
  def estimateCoresOnExec(conf: SparkConf): Int = {
    val executorCoreConfOption = conf.getOption(RapidsPluginUtils.EXECUTOR_CORES_KEY)
    val masterOption = conf.getOption(RapidsPluginUtils.SPARK_MASTER)
    val numCores = masterOption match {
      case Some(m) =>
        m match {
          case "yarn" =>
            executorCoreConfOption.map(_.toInt).getOrElse(1)
          case m if m.startsWith("k8s") =>
            executorCoreConfOption.map(_.toInt).getOrElse(1)
          case m if m.startsWith("spark") =>
            // STANDALONE
            executorCoreConfOption.map(_.toInt).getOrElse(Runtime.getRuntime.availableProcessors)
          case m if m.startsWith("local-cluster") =>
            TrampolineUtil.getCoresInLocalMode(m, conf)
          case m if m.startsWith("local") =>
            TrampolineUtil.getCoresInLocalMode(m, conf)
          case _ =>
            val coresToUse = executorCoreConfOption.map(_.toInt).getOrElse(1)
            logWarning(s"Master: $m is unknown, number of " +
              s"cores is set to $coresToUse")
            coresToUse
        }
      case None =>
        // master not set
        val coresToUse = executorCoreConfOption.map(_.toInt).getOrElse(1)
        logWarning(s"Master is not set, number of cores is set to $coresToUse")
        coresToUse
    }
    logInfo(s"Estimated number of cores is $numCores")
    numCores
  }

  def fixupConfigsOnDriver(conf: SparkConf): Unit = {
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
      if (conf.contains(KRYO_REGISTRATOR_KEY)) {
        if (!KRYO_REGISTRATOR_NAME.equals(conf.get(KRYO_REGISTRATOR_KEY)) ) {
          logWarning("The RAPIDS Accelerator when used with Kryo needs to register some " +
              s"serializers using $KRYO_REGISTRATOR_NAME. Please call it from your registrator " +
              " to let the plugin work properly.")
        } // else it is set and we are good to go
      }  else {
        // We cannot set the kryo key here, it is not early enough to be picked up everywhere
        throw new UnsupportedOperationException("The RAPIDS Accelerator when used with Kryo " +
            "needs to register some serializers. Please set the spark config " +
            s"$KRYO_REGISTRATOR_KEY to $KRYO_REGISTRATOR_NAME or some operations may not work " +
            "properly.")
      }
    } else if (!JAVA_SERIALIZER_NAME.equals(serializer)) {
      throw new UnsupportedOperationException(s"$serializer is not a supported serializer for " +
          s"the RAPIDS Accelerator. Please disable the RAPIDS Accelerator or use a supported " +
          s"serializer ($JAVA_SERIALIZER_NAME, $KRYO_SERIALIZER_NAME).")
    }
    // set driver timezone
    conf.set(RapidsConf.DRIVER_TIMEZONE.key, ZoneId.systemDefault().normalized().toString)

    // If spark.rapids.sql.multiThreadedRead.numThreads is not set explicitly, then we derive it
    // from other settings. Otherwise, we keep the users' setting.
    val numThreadsKey = RapidsConf.MULTITHREAD_READ_NUM_THREADS.key
    if (!conf.contains(numThreadsKey)) {
      // Derive it from spark.executor.cores, since spark.executor.cores is not set on all cluster
      // managers by default, we should judge whether if it's set explicitly.
      if (conf.contains(EXECUTOR_CORES_KEY)) {
        val numThreads = Math.max(RapidsConf.MULTITHREAD_READ_NUM_THREADS_DEFAULT,
          conf.get(EXECUTOR_CORES_KEY).toInt).toString
        conf.set(numThreadsKey, numThreads)
        logWarning(s"$numThreadsKey is set to $numThreads.")
      }
    }

    // If spark.task.resource.gpu.amount is larger than
    // (spark.executor.resource.gpu.amount / spark.executor.cores) then GPUs will be the limiting
    // resource for task scheduling, but we can only output the warning if executor cores is set
    // because this is happening on the driver so the number of cores in the runtime is not
    // relevant
    if (conf.contains(TASK_GPU_AMOUNT_KEY) &&
        conf.contains(EXECUTOR_GPU_AMOUNT_KEY) &&
        conf.contains(EXECUTOR_CORES_KEY)) {
      val taskGpuAmountSetByUser = conf.get(TASK_GPU_AMOUNT_KEY).toDouble
      val executorCores = conf.get(EXECUTOR_CORES_KEY).toDouble
      val executorGpuAmount = conf.get(EXECUTOR_GPU_AMOUNT_KEY).toDouble
      if (executorCores != 0 && taskGpuAmountSetByUser > executorGpuAmount / executorCores) {
        logWarning("The current setting of spark.task.resource.gpu.amount " +
        s"($taskGpuAmountSetByUser) is not ideal to get the best performance from the " +
        "RAPIDS Accelerator plugin. It's recommended to be 1/{executor core count} unless " +
        "you have a special use case.")
      }
    }
  }

  def loadProps(resourceName: String): Map[String, String] = {
    val classLoader = RapidsPluginUtils.getClass.getClassLoader
    val resource = classLoader.getResourceAsStream(resourceName)
    if (resource == null) {
      throw new PluginException(s"Could not find properties file $resourceName in the classpath")
    }
    val props = new Properties
    props.load(resource)
    props.asScala.toMap
  }

  private def loadExtensions[T <: AnyRef](extClass: Class[T], classes: Seq[String]): Seq[T] = {
    classes.flatMap { name =>
      try {
        val klass = TrampolineUtil.classForName[T](name)
        require(extClass.isAssignableFrom(klass),
          s"$name is not a subclass of ${extClass.getName()}.")
        Some(klass.getConstructor().newInstance())
      } catch {
        case _: NoSuchMethodException =>
          throw new NoSuchMethodException(
            s"$name did not have a zero-argument constructor or a" +
              " single-argument constructor that accepts SparkConf. Note: if the class is" +
              " defined inside of another Scala class, then its constructors may accept an" +
              " implicit parameter that references the enclosing class; in this case, you must" +
              " define the class as a top-level class in order to prevent this extra" +
              " parameter from breaking Spark's ability to find a valid constructor.")

        case e: InvocationTargetException =>
          e.getCause() match {
            case uoe: UnsupportedOperationException =>
              logDebug(s"Extension $name not being initialized.", uoe)
              logInfo(s"Extension $name not being initialized.")
              None

            case null => throw e
            case cause => throw cause
          }
      }
    }
  }

  private def getExtraPlugins: Seq[SparkPlugin] = {
    val resourceName = "spark-rapids-extra-plugins"
    val classLoader = RapidsPluginUtils.getClass.getClassLoader
    val resource = classLoader.getResourceAsStream(resourceName)
    if (resource == null) {
      logDebug(s"Could not find file $resourceName in the classpath, not loading extra plugins")
      Seq.empty
    } else {
      val pluginClasses = scala.io.Source.fromInputStream(resource).getLines().toSeq
      loadExtensions(classOf[SparkPlugin], pluginClasses)
    }
  }

  /**
   * Extracts supported GPU architectures from the given properties file
   */
  private def getSupportedGpuArchitectures(props: Map[String, String], origin: String): Set[Int] = {
    props.getOrElse("gpu_architectures", sys.error(s"GPU architectures not found in $origin"))
      .split(";")
      .map(_.toInt)
      .toSet
  }

  /**
   * Checks if the current GPU architecture is supported by the spark-rapids-jni
   * and cuDF libraries.
   */
  def validateGpuArchitecture(): Unit = {
    val gpuArch = Cuda.getComputeCapabilityMajor * 10 + Cuda.getComputeCapabilityMinor
    validateGpuArchitectureInternal(gpuArch,
      getSupportedGpuArchitectures(buildInfoEvent.sparkRapidsJniBuildInfo, JNI_PROPS_FILENAME),
      getSupportedGpuArchitectures(buildInfoEvent.cudfBuildInfo, CUDF_PROPS_FILENAME))
  }

  /**
   * Checks the validity of the provided GPU architecture in the provided architecture set.
   *
   * See: https://docs.nvidia.com/cuda/ampere-compatibility-guide/index.html
   */
  def validateGpuArchitectureInternal(gpuArch: Int, jniSupportedGpuArchs: Set[Int],
      cudfSupportedGpuArchs: Set[Int]): Unit = {
    val supportedGpuArchs = jniSupportedGpuArchs.intersect(cudfSupportedGpuArchs)
    if (supportedGpuArchs.isEmpty) {
      val jniSupportedGpuArchsStr = jniSupportedGpuArchs.toSeq.sorted.mkString(", ")
      val cudfSupportedGpuArchsStr = cudfSupportedGpuArchs.toSeq.sorted.mkString(", ")
      throw new IllegalStateException(s"Compatibility check failed for GPU architecture " +
        s"$gpuArch. Supported GPU architectures by JNI: $jniSupportedGpuArchsStr and " +
        s"cuDF: $cudfSupportedGpuArchsStr. Please report this issue at $SPARK_RAPIDS_REPO_URL." +
        s" This check can be disabled by setting `spark.rapids.skipGpuArchitectureCheck` to" +
        s" `true`, but it may lead to functional failures.")
    }

    val minSupportedGpuArch = supportedGpuArchs.min
    // Check if the device architecture is supported
    if (gpuArch < minSupportedGpuArch) {
      throw new RuntimeException(s"Device architecture $gpuArch is unsupported." +
        s" Minimum supported architecture: $minSupportedGpuArch.")
    }
    val supportedMajorGpuArchs = supportedGpuArchs.map(_ / 10)
    val majorGpuArch = gpuArch / 10
    // Warn the user if the device's major architecture is not available
    if (!supportedMajorGpuArchs.contains(majorGpuArch)) {
      val supportedMajorArchStr = supportedMajorGpuArchs.toSeq.sorted.mkString(", ")
      logWarning(s"No precompiled binaries for device major architecture $majorGpuArch. " +
        "This may lead to expensive JIT compile on startup. " +
        s"Binaries available for architectures $supportedMajorArchStr.")
    }
  }
}

/**
 * The Spark driver plugin provided by the RAPIDS Spark plugin.
 */
class RapidsDriverPlugin extends DriverPlugin with Logging {
  var rapidsShuffleHeartbeatManager: RapidsShuffleHeartbeatManager = null
  private lazy val extraDriverPlugins =
    RapidsPluginUtils.extraPlugins.map(_.driverPlugin()).filterNot(_ == null)

  override def receive(msg: Any): AnyRef = {
    msg match {
      case m: FileCacheLocalityMsg =>
        // handleMsg should not block current thread
        FileCacheLocalityManager.get.handleMsg(m)
      case RapidsExecutorStartupMsg(id) =>
        if (rapidsShuffleHeartbeatManager == null) {
          throw new IllegalStateException(
            s"Rpc message $msg received, but shuffle heartbeat manager not configured.")
        }
        rapidsShuffleHeartbeatManager.registerExecutor(id)
      case RapidsExecutorHeartbeatMsg(id) =>
        if (rapidsShuffleHeartbeatManager == null) {
          throw new IllegalStateException(
            s"Rpc message $msg received, but shuffle heartbeat manager not configured.")
        }
        rapidsShuffleHeartbeatManager.executorHeartbeat(id)
      case m: GpuCoreDumpMsg => GpuCoreDumpHandler.handleMsg(m)
      case m: ProfileMsg => ProfilerOnDriver.handleMsg(m)
      case m => throw new IllegalStateException(s"Unknown message $m")
    }
  }

  override def init(
    sc: SparkContext, pluginContext: PluginContext): java.util.Map[String, String] = {
    val sparkConf = pluginContext.conf
    RapidsPluginUtils.fixupConfigsOnDriver(sparkConf)
    val conf = new RapidsConf(sparkConf)
    RapidsPluginUtils.detectMultipleJars(conf)
    RapidsPluginUtils.logPluginMode(conf)
    GpuCoreDumpHandler.driverInit(sc, conf)
    ProfilerOnDriver.init(sc, conf)

    if (GpuShuffleEnv.isRapidsShuffleAvailable(conf)) {
      GpuShuffleEnv.initShuffleManager()
      if (GpuShuffleEnv.isUCXShuffleAndEarlyStart(conf)) {
        rapidsShuffleHeartbeatManager =
          new RapidsShuffleHeartbeatManager(
            conf.shuffleTransportEarlyStartHeartbeatInterval,
            conf.shuffleTransportEarlyStartHeartbeatTimeout)
      }
    }

    FileCacheLocalityManager.init(sc)

    logDebug("Loading extra driver plugins: " +
      s"${extraDriverPlugins.map(_.getClass.getName).mkString(",")}")
    extraDriverPlugins.foreach(_.init(sc, pluginContext))
    TrampolineUtil.postEvent(sc, buildInfoEvent)
    conf.rapidsConfMap
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    extraDriverPlugins.foreach(_.registerMetrics(appId, pluginContext))
  }

  override def shutdown(): Unit = {
    extraDriverPlugins.foreach(_.shutdown())
    FileCacheLocalityManager.shutdown()
  }
}

/**
 * The Spark executor plugin provided by the RAPIDS Spark plugin.
 */
class RapidsExecutorPlugin extends ExecutorPlugin with Logging {
  var rapidsShuffleHeartbeatEndpoint: RapidsShuffleHeartbeatEndpoint = null
  private lazy val extraExecutorPlugins =
    RapidsPluginUtils.extraPlugins.map(_.executorPlugin()).filterNot(_ == null)
  private val activeTaskNvtx = new ConcurrentHashMap[Thread, NvtxRange]()

  override def init(
      pluginContext: PluginContext,
      extraConf: java.util.Map[String, String]): Unit = {
    try {
      // if configured, re-register checking leaks hook.
      reRegisterCheckLeakHook()

      val sparkConf = pluginContext.conf()
      val numCores = RapidsPluginUtils.estimateCoresOnExec(sparkConf)
      val conf = new RapidsConf(extraConf.asScala.toMap)
      ProfilerOnExecutor.init(pluginContext, conf)

      // Checks if the current GPU architecture is supported by the
      // spark-rapids-jni and cuDF libraries.
      // Note: We allow this check to be skipped for off-chance cases.
      if (!conf.skipGpuArchCheck) {
        RapidsPluginUtils.validateGpuArchitecture()
      }

      // Fail if there are multiple plugin jars in the classpath.
      RapidsPluginUtils.detectMultipleJars(conf)

      // Compare if the cudf version mentioned in the classpath is equal to the version which
      // plugin expects. If there is a version mismatch, throw error. This check can be disabled
      // by setting this config spark.rapids.cudfVersionOverride=true
      checkCudfVersion(conf)

      // Validate driver and executor time zone are same if the driver time zone is supported by
      // the plugin.
      val driverTimezone = conf.driverTimeZone match {
        case Some(value) => ZoneId.of(value)
        case None => throw new RuntimeException(s"Driver time zone cannot be determined.")
      }
      val executorTimezone = ZoneId.systemDefault()
      if (executorTimezone.normalized() != driverTimezone.normalized()) {
        throw new RuntimeException(s" Driver and executor timezone mismatch. " +
          s"Driver timezone is $driverTimezone and executor timezone is " +
          s"$executorTimezone. Set executor timezone to $driverTimezone.")
      }

      GpuCoreDumpHandler.executorInit(conf, pluginContext)

      // we rely on the Rapids Plugin being run with 1 GPU per executor so we can initialize
      // on executor startup.
      if (!GpuDeviceManager.rmmTaskInitEnabled) {
        logInfo("Initializing memory from Executor Plugin")
        GpuDeviceManager.initializeGpuAndMemory(pluginContext.resources().asScala.toMap, conf,
          numCores)
        if (GpuShuffleEnv.isRapidsShuffleAvailable(conf)) {
          GpuShuffleEnv.initShuffleManager()
          if (GpuShuffleEnv.isUCXShuffleAndEarlyStart(conf)) {
            logInfo("Initializing shuffle manager heartbeats")
            rapidsShuffleHeartbeatEndpoint = new RapidsShuffleHeartbeatEndpoint(pluginContext, conf)
            rapidsShuffleHeartbeatEndpoint.registerShuffleHeartbeat()
          }
        }
      }

      logDebug("Loading extra executor plugins: " +
        s"${extraExecutorPlugins.map(_.getClass.getName).mkString(",")}")
      extraExecutorPlugins.foreach(_.init(pluginContext, extraConf))
      GpuSemaphore.initialize()
      FileCache.init(pluginContext)
    } catch {
      // Exceptions in executor plugin can cause a single thread to die but the executor process
      // sticks around without any useful info until it hearbeat times out. Print what happened
      // and exit immediately.
      case e: CudaException =>
        logError("Exception in the executor plugin, shutting down!", e)
        logGpuDebugInfoAndExit(systemExitCode = 1)
      case e: Throwable =>
        logError("Exception in the executor plugin, shutting down!", e)
        System.exit(1)
    }
  }

  /**
   * Re-register leaks checking hook if configured.
   */
  private def reRegisterCheckLeakHook(): Unit = {
    // DEFAULT_SHUTDOWN_THREAD in MemoryCleaner is responsible to check the leaks at shutdown time,
    // it expects all other hooks are done before the checking
    // as other hooks will close some resources.

    if (MemoryCleaner.configuredDefaultShutdownHook) {
      // Shutdown hooks are executed concurrently in JVM, and there is no execution order guarantee.
      // See the doc of `Runtime.addShutdownHook`.
      // Here we should wait Spark hooks to be done, or a false leak will be detected.
      // See issue: https://github.com/NVIDIA/spark-rapids/issues/5854
      //
      // Here use `Spark ShutdownHookManager` to manage hooks with priority.
      // 20 priority is small enough, will run after Spark hooks.
      TrampolineUtil.addShutdownHook(20, MemoryCleaner.removeDefaultShutdownHook())
    }
  }

  private def checkCudfVersion(conf: RapidsConf): Unit = {
    try {
      val expectedCudfVersion = buildInfoEvent.sparkRapidsBuildInfo.getOrElse("cudf_version",
        throw CudfVersionMismatchException("Could not find cudf version in " +
            RapidsPluginUtils.PLUGIN_PROPS_FILENAME))

      val cudfVersion = buildInfoEvent.cudfBuildInfo.getOrElse("version",
        throw CudfVersionMismatchException("Could not find cudf version in " +
            RapidsPluginUtils.CUDF_PROPS_FILENAME))

      // compare cudf version in the classpath with the cudf version expected by plugin
      if (!RapidsExecutorPlugin.cudfVersionSatisfied(expectedCudfVersion, cudfVersion)) {
        throw CudfVersionMismatchException(s"Found cudf version $cudfVersion, RAPIDS Accelerator " +
            s"expects $expectedCudfVersion")
      }
    } catch {
      case x: PluginException if conf.cudfVersionOverride =>
        logWarning(s"Ignoring error due to ${RapidsConf.CUDF_VERSION_OVERRIDE.key}=true: " +
            s"${x.getMessage}")
    }
  }

  // Wait for command spawned via Process
  private def waitForProcess(cmd: Process, durationMs: Long): Option[Int] = {
    val endTime = System.currentTimeMillis() + durationMs
    do {
      Thread.sleep(10)
      if (!cmd.isAlive()) {
        return Some(cmd.exitValue())
      }
    } while (System.currentTimeMillis() < endTime)
    // Timed out
    cmd.destroy()
    None
  }

  // Try to run nvidia-smi when task fails due to a cuda exception.
  private def logGpuDebugInfoAndExit(systemExitCode: Int) = synchronized {
    try {
      val nvidiaSmiStdout = new StringBuilder
      val nvidiaSmiStderr = new StringBuilder
      val cmd = "nvidia-smi".run(
        ProcessLogger(s => nvidiaSmiStdout.append(s + "\n"), s => nvidiaSmiStderr.append(s + "\n")))
      waitForProcess(cmd, 10000) match {
        case Some(exitStatus) =>
          if (exitStatus == 0) {
            logWarning("nvidia-smi:\n" + nvidiaSmiStdout)
          } else {
            logWarning("nvidia-smi failed with: " + nvidiaSmiStdout + nvidiaSmiStderr)
          }
        case None => logWarning("nvidia-smi command timed out")
      }
    } catch {
      case e: Throwable =>
        logWarning("nvidia-smi process failed", e)
    }
    System.exit(systemExitCode)
  }

  override def shutdown(): Unit = {
    GpuTimeZoneDB.shutdown()
    GpuSemaphore.shutdown()
    PythonWorkerSemaphore.shutdown()
    GpuDeviceManager.shutdown()
    ProfilerOnExecutor.shutdown()
    Option(rapidsShuffleHeartbeatEndpoint).foreach(_.close())
    extraExecutorPlugins.foreach(_.shutdown())
    FileCache.shutdown()
    GpuCoreDumpHandler.shutdown()
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    def containsCudaFatalException(e: Throwable): Boolean = {
      ExceptionUtils.getThrowableList(e).asScala.exists(e => e.isInstanceOf[CudaFatalException])
    }
    failureReason match {
      case ef: ExceptionFailure =>
        ef.exception match {
          case Some(e) if containsCudaFatalException(e) =>
            logError("Stopping the Executor based on exception being a fatal CUDA error: " +
              s"${ef.toErrorString}")
            GpuCoreDumpHandler.waitForDump(timeoutSecs = 60)
            logGpuDebugInfoAndExit(systemExitCode = 20)
          case Some(_: CudaException) =>
            logDebug(s"Executor onTaskFailed because of a non-fatal CUDA error: " +
              s"${ef.toErrorString}")
          case Some(_: CudfException) =>
            logDebug(s"Executor onTaskFailed because of a CUDF error: ${ef.toErrorString}")
          case _ =>
            logDebug(s"Executor onTaskFailed: ${ef.toErrorString}")
        }
      case other =>
        logDebug(s"Executor onTaskFailed: ${other.toString}")
    }
    extraExecutorPlugins.foreach(_.onTaskFailed(failureReason))
    endTaskNvtx()
  }

  override def onTaskStart(): Unit = {
    startTaskNvtx(TaskContext.get)
    extraExecutorPlugins.foreach(_.onTaskStart())
    ProfilerOnExecutor.onTaskStart()
  }

  override def onTaskSucceeded(): Unit = {
    extraExecutorPlugins.foreach(_.onTaskSucceeded())
    endTaskNvtx()
  }

  private def startTaskNvtx(taskCtx: TaskContext): Unit = {
    val stageId = taskCtx.stageId()
    val taskAttemptId = taskCtx.taskAttemptId()
    val attemptNumber = taskCtx.attemptNumber()
    activeTaskNvtx.put(Thread.currentThread(),
      new NvtxRange(s"Stage $stageId Task $taskAttemptId-$attemptNumber", NvtxColor.DARK_GREEN))
  }

  private def endTaskNvtx(): Unit = {
    val nvtx = activeTaskNvtx.remove(Thread.currentThread())
    if (nvtx != null) {
      nvtx.close()
    }
  }
}

object RapidsExecutorPlugin {
  /**
   * Return true if the expected cudf version is satisfied by the actual version found.
   * The version is satisfied if the major and minor versions match exactly. If there is a requested
   * patch version then the actual patch version must be greater than or equal.
   * For example, version 7.1 is not satisfied by version 7.2, but version 7.1 is satisfied by
   * version 7.1.1.
   * If the expected cudf version is a specified 'timestamp-seq' one, then it is satisfied by
   * the SNAPSHOT version.
   * For example, version 7.1-yyyymmdd.hhmmss-seq is satisfied by version 7.1-SNAPSHOT.
   */
  def cudfVersionSatisfied(expected: String, actual: String): Boolean = {
    val expHyphen = if (expected.indexOf('-') >= 0) expected.indexOf('-') else expected.length
    val actHyphen = if (actual.indexOf('-') >= 0) actual.indexOf('-') else actual.length
    if (actual.substring(actHyphen) != expected.substring(expHyphen) &&
      !(actual.substring(actHyphen) == "-SNAPSHOT" &&
        expected.substring(expHyphen).matches("-([0-9]{8}).([0-9]{6})-([1-9][0-9]*)"))) {
      return false
    }

    val (expMajorMinor, expPatch) = expected.substring(0, expHyphen).split('.').splitAt(2)
    val (actMajorMinor, actPatch) = actual.substring(0, actHyphen).split('.').splitAt(2)
    actMajorMinor.startsWith(expMajorMinor) && {
      val expPatchInts = expPatch.map(_.toInt)
      val actPatchInts = actPatch.map(v => Try(v.toInt).getOrElse(Int.MinValue))
      val zipped = expPatchInts.zipAll(actPatchInts, 0, 0)
      zipped.forall { case (e, a) => e <= a }
    }
  }
}
