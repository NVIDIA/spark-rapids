/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import java.net.URL

import com.nvidia.spark.GpuCachedBatchSerializer
import com.nvidia.spark.rapids.delta.DeltaProbe
import com.nvidia.spark.rapids.iceberg.IcebergProvider
import org.apache.commons.lang3.reflect.MethodUtils
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.{SPARK_BRANCH, SPARK_BUILD_DATE, SPARK_BUILD_USER, SPARK_REPO_URL, SPARK_REVISION, SPARK_VERSION, SparkConf, SparkEnv}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin}
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.rapids.execution.UnshimmedTrampolineUtil
import org.apache.spark.util.MutableURLClassLoader

/*
    Plugin jar uses non-standard class file layout. It consists of three types of areas,
    "parallel worlds" in the JDK's com.sun.istack.internal.tools.ParallelWorldClassLoader parlance

    1. a few publicly documented classes in the conventional layout at the top
    2. a large fraction of classes whose bytecode is identical under all supported Spark versions
       in spark3xx-common
    3. a smaller fraction of classes that differ under one of the supported Spark versions

    com/nvidia/spark/SQLPlugin.class

    spark3xx-common/com/nvidia/spark/rapids/CastExprMeta.class

    spark311/org/apache/spark/sql/rapids/GpuUnaryMinus.class
    spark320/org/apache/spark/sql/rapids/GpuUnaryMinus.class

    Each shim can see a consistent parallel world without conflicts by referencing
    only one conflicting directory.

    E.g., Spark 3.2.0 Shim will use

    jar:file:/home/spark/rapids-4-spark_2.12-23.06.0.jar!/spark3xx-common/
    jar:file:/home/spark/rapids-4-spark_2.12-23.06.0.jar!/spark320/

    Spark 3.1.1 will use

    jar:file:/home/spark/rapids-4-spark_2.12-23.06.0.jar!/spark3xx-common/
    jar:file:/home/spark/rapids-4-spark_2.12-23.06.0.jar!/spark311/

    Using these Jar URL's allows referencing different bytecode produced from identical sources
    by incompatible Scala / Spark dependencies.
 */
object ShimLoader extends Logging {
  logDebug(s"ShimLoader object instance: $this loaded by ${getClass.getClassLoader}")
  private val shimRootURL = {
    val thisClassFile = getClass.getName.replace(".", "/") + ".class"
    val url = getClass.getClassLoader.getResource(thisClassFile)
    val urlStr = url.toString
    val rootUrlStr = urlStr.substring(0, urlStr.length - thisClassFile.length)
    new URL(rootUrlStr)
  }

  private val shimCommonURL = new URL(s"${shimRootURL.toString}spark3xx-common/")
  @volatile private var shimProviderClass: String = _
  @volatile private var shimProvider: SparkShimServiceProvider = _
  @volatile private var shimURL: URL = _
  @volatile private var pluginClassLoader: ClassLoader = _
  @volatile private var conventionalSingleShimJarDetected: Boolean = _

  // REPL-only logic
  @volatile private var tmpClassLoader: MutableURLClassLoader = _

  private def shimId: String = shimIdFromPackageName(shimProviderClass)

  private def urlsForSparkClassLoader = Seq(
    shimCommonURL,
    shimURL
  )

  // defensively call findShimProvider logic on all entry points to avoid uninitialized
  // this won't be necessary if we can upstream changes to the plugin and shuffle
  // manager loading changes to Apache Spark
  private def initShimProviderIfNeeded(): Unit = {
    if (shimURL == null) {
      findShimProvider()
    }
  }

  // Ideally we would like to expose a simple Boolean config instead of having to document
  // per-shim ShuffleManager implementations:
  // https://github.com/NVIDIA/spark-rapids/blob/branch-21.08/docs/additional-functionality/
  // rapids-shuffle.md#spark-app-configuration
  //
  // This is not possible at the current stage of the shim layer rewrite because of the combination
  // of the following two reasons:
  // 1) Spark processes ShuffleManager config before any of the plugin code initialized
  // 2) We can't combine the implementation of the ShuffleManager trait for different Spark
  //    versions in the same Scala class. A method was changed to final
  //    https://github.com/apache/spark/blame/v3.2.0-rc2/core/src/main/scala/
  //    org/apache/spark/shuffle/ShuffleManager.scala#L57
  //
  //    ShuffleBlockResolver implementation for 3.1 has MergedBlockMeta in signatures
  //    missing in the prior versions leading to CNF when loaded in earlier version
  //
  def getRapidsShuffleManagerClass: String = {
    initShimProviderIfNeeded()
    s"com.nvidia.spark.rapids.$shimId.RapidsShuffleManager"
  }

  def getRapidsShuffleInternalClass: String = {
    initShimProviderIfNeeded()
    s"org.apache.spark.sql.rapids.shims.$shimId.RapidsShuffleInternalManager"
  }

  @tailrec
  private def findURLClassLoader(classLoader: ClassLoader): Option[ClassLoader] = {
    // walk up the classloader hierarchy until we hit a classloader we can mutate
    // in the upstream Spark, non-REPL/batch mode serdeClassLoader is already mutable
    // in REPL use-cases, and blackbox Spark apps it may take several iterations

    // ignore different flavors of URL classloaders in different REPLs
    // brute-force call addURL using reflection
    classLoader match {
      case nullClassLoader if nullClassLoader == null =>
        logInfo("findURLClassLoader failed to locate a mutable classloader")
        None
      case urlCl: java.net.URLClassLoader =>
        // fast path
        logInfo(s"findURLClassLoader found a URLClassLoader $urlCl")
        Option(urlCl)
      case replCl if replCl.getClass.getName == "org.apache.spark.repl.ExecutorClassLoader" =>
        // https://issues.apache.org/jira/browse/SPARK-18646
        val parentLoader = MethodUtils.invokeMethod(replCl, true, "parentLoader")
          .asInstanceOf[ClassLoader]
        logInfo(s"findURLClassLoader found $replCl, trying parentLoader=$parentLoader")
        findURLClassLoader(parentLoader)
      case urlAddable: ClassLoader if null != MethodUtils.getMatchingMethod(
          urlAddable.getClass, "addURL", classOf[java.net.URL]) =>
        // slow defensive path
        logInfo(s"findURLClassLoader found a urLAddable classloader $urlAddable")
        Option(urlAddable)
      case root if root.getParent == null || root.getParent == root =>
        logInfo(s"findURLClassLoader hit the Boostrap classloader $root, " +
          s"failed to find a mutable classloader!")
        None
      case cl =>
        val parentClassLoader = cl.getParent
        logInfo(s"findURLClassLoader found an immutable $cl, trying parent=$parentClassLoader")
        findURLClassLoader(parentClassLoader)
    }
  }

  private def updateSparkClassLoader(): Unit = {
    findURLClassLoader(UnshimmedTrampolineUtil.sparkClassLoader).foreach { urlAddable =>
      urlsForSparkClassLoader.foreach { url =>
        if (!conventionalSingleShimJarDetected) {
          logInfo(s"Updating spark classloader $urlAddable with the URLs: " +
            urlsForSparkClassLoader.mkString(", "))
          MethodUtils.invokeMethod(urlAddable, true, "addURL", url)
          logInfo(s"Spark classLoader $urlAddable updated successfully")
          urlAddable match {
            case urlCl: java.net.URLClassLoader =>
              if (!urlCl.getURLs.contains(shimCommonURL)) {
                // infeasible, defensive diagnostics
                logWarning(s"Didn't find expected URL $shimCommonURL in the spark " +
                  s"classloader $urlCl although addURL succeeded, maybe pushed up to the " +
                  s"parent classloader ${urlCl.getParent}")
              }
            case _ => Unit
          }
        }
      }
      pluginClassLoader = urlAddable
    }
  }

  def getShimClassLoader(): ClassLoader = {
    initShimProviderIfNeeded()
    if (pluginClassLoader == null) {
      updateSparkClassLoader()
    }
    if (pluginClassLoader == null) {
      if (tmpClassLoader == null) {
        tmpClassLoader = new MutableURLClassLoader(Array(shimURL, shimCommonURL),
          getClass.getClassLoader)
        logWarning("Found an unexpected context classloader " +
            s"${Thread.currentThread().getContextClassLoader}. We will try to recover from this, " +
            "but it may cause class loading problems.")
      }
      tmpClassLoader
    } else {
      pluginClassLoader
    }
  }

  private val SERVICE_LOADER_PREFIX = "META-INF/services/"

  private def detectShimProvider(): String = {
    val sparkVersion = getSparkVersion
    logInfo(s"Loading shim for Spark version: $sparkVersion")
    logInfo("Complete Spark build info: " + sparkBuildInfo.mkString(", "))

    val thisClassLoader = getClass.getClassLoader

    // Emulating service loader manually because we have a non-standard jar layout for classes
    // when we pass a classloader to https://docs.oracle.com/javase/8/docs/api/java/util/
    // ServiceLoader.html#load-java.lang.Class-java.lang.ClassLoader-
    // it expects META-INF/services at the normal root locations (OK)
    // and provider classes under the normal root entry as well. The latter is not OK because we
    // want to minimize the use of reflection and prevent leaking the provider to a conventional
    // classloader.
    //
    // Alternatively, we could use a ChildFirstClassloader implementation. However, this means that
    // ShimServiceProvider API definition is not shared via parent and we run
    // into ClassCastExceptions. If we find a way to solve this then we can revert to ServiceLoader

    // IMPORTANT don't use RapidsConf as it transitively references classes that must remain
    // in parallel worlds
    val shimServiceProviderOverrideClassName = Option(SparkEnv.get) // Spark-less RapidsConf.help
      .flatMap(_.conf.getOption("spark.rapids.shims-provider-override"))
    shimServiceProviderOverrideClassName.foreach { shimProviderClass =>
      logWarning(s"Overriding Spark shims provider to $shimProviderClass. " +
        "This may be an untested configuration!")
    }

    val serviceProviderListPath = SERVICE_LOADER_PREFIX + classOf[SparkShimServiceProvider].getName
    val serviceProviderList = shimServiceProviderOverrideClassName
      .map(clsName => Seq(clsName)).getOrElse {
        thisClassLoader.getResources(serviceProviderListPath)
          .asScala.map(scala.io.Source.fromURL)
          .flatMap(_.getLines())
          .toSeq
      }

    assert(serviceProviderList.nonEmpty, "Classpath should contain the resource for " +
        serviceProviderListPath)

    val numShimServiceProviders = serviceProviderList.size
    val shimServiceProviderOpt = serviceProviderList.flatMap { shimServiceProviderStr =>
      val mask = shimIdFromPackageName(shimServiceProviderStr)
      try {
        val shimURL = new java.net.URL(s"${shimRootURL.toString}$mask/")
        val shimClassLoader = new MutableURLClassLoader(Array(shimURL, shimCommonURL),
          thisClassLoader)
        val shimClass = Try[java.lang.Class[_]] {
          val ret = thisClassLoader.loadClass(shimServiceProviderStr)
          if (numShimServiceProviders == 1) {
            conventionalSingleShimJarDetected = true
            logInfo("Conventional shim jar layout for a single Spark verision detected")
          }
          ret
        }.getOrElse(shimClassLoader.loadClass(shimServiceProviderStr))
        Option(
          (ShimReflectionUtils.instantiateClass(shimClass).asInstanceOf[SparkShimServiceProvider],
            shimURL)
        )
      } catch {
        case cnf: ClassNotFoundException =>
          logDebug(cnf + ": Could not load the provider, likely a dev build", cnf)
          None
      }
    }.find { case (shimServiceProvider, _) =>
      shimServiceProviderOverrideClassName.nonEmpty ||
        shimServiceProvider.matchesVersion(sparkVersion)
    }.map { case (inst, url) =>
      shimURL = url
      shimProvider = inst
      // this class will be loaded again by the real executor classloader
      inst.getClass.getName
    }

    shimServiceProviderOpt.getOrElse {
        throw new IllegalArgumentException(s"Could not find Spark Shim Loader for $sparkVersion")
    }
  }

  private def shimIdFromPackageName(shimServiceProviderStr: String) = {
    shimServiceProviderStr.split('.').takeRight(2).head
  }

  private def findShimProvider(): String = {
    // TODO restore support for shim provider override
    if (shimProviderClass == null) {
      shimProviderClass = detectShimProvider()
    }
    shimProviderClass
  }

  def getShimVersion: ShimVersion = {
    initShimProviderIfNeeded()
    shimProvider.getShimVersion
  }

  def getSparkVersion: String = {
    // hack for databricks, try to find something more reliable?
    if (SPARK_BUILD_USER.equals("Databricks")) {
      SPARK_VERSION + "-databricks"
    } else {
      SPARK_VERSION
    }
  }

  private def sparkBuildInfo = Seq(
    getSparkVersion,
    SPARK_REPO_URL,
    SPARK_BRANCH,
    SPARK_REVISION,
    SPARK_BUILD_DATE
  )

  //
  // Reflection-based API with Spark to switch the classloader used by the caller
  //

  def newOptimizerClass(className: String): Optimizer = {
    ShimReflectionUtils.newInstanceOf[Optimizer](className)
  }

  def newInternalShuffleManager(conf: SparkConf, isDriver: Boolean): Any = {
    val shuffleClassLoader = getShimClassLoader()
    val shuffleClassName = getRapidsShuffleInternalClass
    val shuffleClass = shuffleClassLoader.loadClass(shuffleClassName)
    shuffleClass.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
  }

  def newDriverPlugin(): DriverPlugin = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.RapidsDriverPlugin")
  }

  def newExecutorPlugin(): ExecutorPlugin = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.RapidsExecutorPlugin")
  }

  def newColumnarOverrideRules(): ColumnarRule = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.ColumnarOverrideRules")
  }

  def newGpuQueryStagePrepOverrides(): Rule[SparkPlan] = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.GpuQueryStagePrepOverrides")
  }

  def newUdfLogicalPlanRules(): Rule[LogicalPlan] = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.udf.LogicalPlanRules")
  }

  def newStrategyRules(): Strategy = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.StrategyRules")
  }

  def newInternalExclusiveModeGpuDiscoveryPlugin(): ResourceDiscoveryPlugin = {
    ShimReflectionUtils.
      newInstanceOf("com.nvidia.spark.rapids.InternalExclusiveModeGpuDiscoveryPlugin")
  }

  def newParquetCachedBatchSerializer(): GpuCachedBatchSerializer = {
    ShimReflectionUtils.newInstanceOf("com.nvidia.spark.rapids.ParquetCachedBatchSerializer")
  }

  def loadColumnarRDD(): Class[_] = {
    ShimReflectionUtils.
      loadClass("org.apache.spark.sql.rapids.execution.InternalColumnarRddConverter")
  }

  def newExplainPlan(): ExplainPlanBase = {
    ShimReflectionUtils.newInstanceOf[ExplainPlanBase]("com.nvidia.spark.rapids.ExplainPlanImpl")
  }

  def newHiveProvider(): HiveProvider= {
    ShimReflectionUtils.
      newInstanceOf[HiveProvider]("org.apache.spark.sql.hive.rapids.HiveProviderImpl")
  }

  def newAvroProvider(): AvroProvider = ShimReflectionUtils.newInstanceOf[AvroProvider](
    "org.apache.spark.sql.rapids.AvroProviderImpl")

  def newDeltaProbe(): DeltaProbe = ShimReflectionUtils.newInstanceOf[DeltaProbe](
    "com.nvidia.spark.rapids.delta.DeltaProbeImpl")

  def newIcebergProvider(): IcebergProvider = ShimReflectionUtils.newInstanceOf[IcebergProvider](
    "com.nvidia.spark.rapids.iceberg.IcebergProviderImpl")

  def newPlanShims(): PlanShims = ShimReflectionUtils.newInstanceOf[PlanShims](
    "com.nvidia.spark.rapids.shims.PlanShimsImpl"
  )

  def loadGpuColumnVector(): Class[_] = {
    ShimReflectionUtils.loadClass("com.nvidia.spark.rapids.GpuColumnVector")
  }
}
