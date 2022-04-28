/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import org.apache.commons.lang3.reflect.MethodUtils
import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.spark.{SPARK_BRANCH, SPARK_BUILD_DATE, SPARK_BUILD_USER, SPARK_REPO_URL, SPARK_REVISION, SPARK_VERSION, SparkConf, SparkEnv}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin}
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.util.{MutableURLClassLoader, ParentClassLoader}

/*
    Plugin jar uses non-standard class file layout. It consists of three types of areas,
    "parallel worlds" in the JDK's com.sun.istack.internal.tools.ParallelWorldClassLoader parlance

    1. a few publicly documented classes in the conventional layout at the top
    2. a large fraction of classes whose bytecode is identical under all supported Spark versions
       in spark3xx-common
    3. a smaller fraction of classes that differ under one of the supported Spark versions

    com/nvidia/spark/SQLPlugin.class

    spark3xx-common/com/nvidia/spark/rapids/CastExprMeta.class

    spark301/org/apache/spark/sql/rapids/GpuUnaryMinus.class
    spark311/org/apache/spark/sql/rapids/GpuUnaryMinus.class
    spark320/org/apache/spark/sql/rapids/GpuUnaryMinus.class

    Each shim can see a consistent parallel world without conflicts by referencing
    only one conflicting directory.

    E.g., Spark 3.2.0 Shim will use

    jar:file:/home/spark/rapids-4-spark_2.12-21.10.jar!/spark3xx-common/
    jar:file:/home/spark/rapids-4-spark_2.12-21.10.jar!/spark320/

    Spark 3.1.1 will use

    jar:file:/home/spark/rapids-4-spark_2.12-21.10.jar!/spark3xx-common/
    jar:file:/home/spark/rapids-4-spark_2.12-21.10.jar!/spark311/

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
  @volatile private var sparkShims: SparkShims = _
  @volatile private var shimURL: URL = _
  @volatile private var pluginClassLoader: ClassLoader = _

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

  private def serializerClassloader(): Option[ClassLoader] = {
    // Hypothesis: serializer is the most universal way to intercept classloaders

    // https://github.com/apache/spark/blob/master/core/src/main/scala/
    // org/apache/spark/serializer/JavaSerializer.scala#L147

    // https://github.com/apache/spark/blob/master/core/src/main/scala/
    // org/apache/spark/serializer/KryoSerializer.scala#L134

    Option(SparkEnv.get)
      .flatMap {
        case env if !env.conf.get("spark.rapids.force.caller.classloader",
          true.toString).toBoolean => Option(env.serializer)
        case _ =>
          logInfo("Forcing shim caller classloader update (default behavior). " +
            "If it causes issues with userClassPathFirst, set " +
            "spark.rapids.force.caller.classloader to false!")
          None
      }
      .flatMap { serializer =>
        logInfo("Looking for a mutable classloader (defaultClassLoader) in SparkEnv.serializer " +
          serializer)
        // scalac generates accessor methods
        val serdeClassLoader = MethodUtils
          .invokeMethod(serializer, true, "defaultClassLoader")
          .asInstanceOf[Option[ClassLoader]]
          .getOrElse {
            val threadContextClassLoader = Thread.currentThread().getContextClassLoader
            logInfo(s"No defaultClassLoader found in $serializer, falling back " +
              s"on Thread context classloader: " + threadContextClassLoader)
            threadContextClassLoader
          }

        logInfo("Extracted Spark classloader from SparkEnv.serializer " + serdeClassLoader)
        findURLClassLoader(serdeClassLoader)
      }.orElse {
        val shimLoaderCallerCl = getClass.getClassLoader
        logInfo("Falling back on ShimLoader caller's classloader " + shimLoaderCallerCl)
        Option(shimLoaderCallerCl)
      }
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
    // TODO propose a proper addClassPathURL API to Spark similar to addJar but
    //  accepting non-file-based URI
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    Option(contextClassLoader).collect {
      case mutable: MutableURLClassLoader => mutable
      case replCL if replCL.getClass.getName == "org.apache.spark.repl.ExecutorClassLoader" =>
        val parentLoaderField = replCL.getClass.getDeclaredMethod("parentLoader")
        val parentLoader = parentLoaderField.invoke(replCL).asInstanceOf[ParentClassLoader]
        parentLoader.getParent.asInstanceOf[MutableURLClassLoader]
    }.foreach { mutable =>
      // MutableURLClassloader dedupes for us
      pluginClassLoader = contextClassLoader
      mutable.addURL(shimURL)
      mutable.addURL(shimCommonURL)
    }
  }

  private def getShimClassLoader(): ClassLoader = {
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
      }

    assert(serviceProviderList.nonEmpty, "Classpath should contain the resource for " +
        serviceProviderListPath)

    val shimServiceProviderOpt = serviceProviderList.flatMap { shimServiceProviderStr =>
      val mask = shimIdFromPackageName(shimServiceProviderStr)
      try {
        val shimURL = new java.net.URL(s"${shimRootURL.toString}$mask/")
        val shimClassLoader = new MutableURLClassLoader(Array(shimURL, shimCommonURL),
          thisClassLoader)
        val shimClass = shimClassLoader.loadClass(shimServiceProviderStr)
        Option(
          (instantiateClass(shimClass).asInstanceOf[SparkShimServiceProvider], shimURL)
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

  def getSparkShims: SparkShims = {
    if (sparkShims == null) {
      sparkShims = newInstanceOf[SparkShimServiceProvider](findShimProvider()).buildShim
    }
    sparkShims
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

  // TODO broken right now, check if this can be supported with parallel worlds
  // it implies the prerequisite of having such a class in the conventional root jar entry
  // - or the necessity of an additional parameter for specifying the shim subdirectory
  // - or enforcing the convention the class file parent directory is the shimId that is also
  //   a top entry e.g. /spark301/com/nvidia/test/shim/spark301/Spark301Shims.class
  def setSparkShimProviderClass(classname: String): Unit = {
    shimProviderClass = classname
  }

  def loadClass(className: String): Class[_] = {
    val loader = getShimClassLoader()
    logDebug(s"Loading $className using $loader with the parent loader ${loader.getParent}")
    loader.loadClass(className)
  }

  def newInstanceOf[T](className: String): T = {
    instantiateClass(loadClass(className)).asInstanceOf[T]
  }

  // avoid cached constructors
  private def instantiateClass[T](cls: Class[T]): T = {
    logDebug(s"Instantiate ${cls.getName} using classloader " + cls.getClassLoader)
    cls.getClassLoader match {
      case urcCl: java.net.URLClassLoader =>
        logDebug("urls " + urcCl.getURLs.mkString("\n"))
      case _ =>
    }
    val constructor = cls.getConstructor()
    constructor.newInstance()
  }


  //
  // Reflection-based API with Spark to switch the classloader used by the caller
  //

  def newInternalShuffleManager(conf: SparkConf, isDriver: Boolean): Any = {
    val shuffleClassLoader = getShimClassLoader()
    val shuffleClassName = getRapidsShuffleInternalClass
    val shuffleClass = shuffleClassLoader.loadClass(shuffleClassName)
    shuffleClass.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
  }

  def newDriverPlugin(): DriverPlugin = {
    newInstanceOf("com.nvidia.spark.rapids.RapidsDriverPlugin")
  }

  def newExecutorPlugin(): ExecutorPlugin = {
    newInstanceOf("com.nvidia.spark.rapids.RapidsExecutorPlugin")
  }

  def newColumnarOverrideRules(): ColumnarRule = {
    newInstanceOf("com.nvidia.spark.rapids.ColumnarOverrideRules")
  }

  def newGpuQueryStagePrepOverrides(): Rule[SparkPlan] = {
    newInstanceOf("com.nvidia.spark.rapids.GpuQueryStagePrepOverrides")
  }

  def newUdfLogicalPlanRules(): Rule[LogicalPlan] = {
    newInstanceOf("com.nvidia.spark.udf.LogicalPlanRules")
  }

  def newInternalExclusiveModeGpuDiscoveryPlugin(): ResourceDiscoveryPlugin = {
    newInstanceOf("com.nvidia.spark.rapids.InternalExclusiveModeGpuDiscoveryPlugin")
  }

  def loadColumnarRDD(): Class[_] = {
    loadClass("org.apache.spark.sql.rapids.execution.InternalColumnarRddConverter")
  }
}
