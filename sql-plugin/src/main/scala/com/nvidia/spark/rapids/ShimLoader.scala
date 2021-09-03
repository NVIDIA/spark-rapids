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

import scala.collection.JavaConverters._

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.VisibleShuffleManager
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
  logDebug(s"ShimLoader object instance: ${this} loaded by ${getClass.getClassLoader}")
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

  def shimId: String = shimIdFromPackageName(shimProviderClass)

  def getRapidsShuffleManagerClass: String = {
    findShimProvider()
    s"com.nvidia.spark.rapids.$shimId.RapidsShuffleManager"
  }

  def getRapidsShuffleInternal: String = {
    findShimProvider()
    s"org.apache.spark.sql.rapids.shims.$shimId.RapidsShuffleInternalManager"
  }

  private def updateSparkClassLoader(): Unit = {
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
    if (shimURL == null) {
      findShimProvider()
    }
    if (pluginClassLoader == null) {
      updateSparkClassLoader()
    }
    if (pluginClassLoader == null) {
      if (tmpClassLoader == null) {
        tmpClassLoader = new MutableURLClassLoader(Array(shimURL, shimCommonURL),
          getClass.getClassLoader)
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

    val thisClassLoader = getClass.getClassLoader
    // Emulating service loader manually because we have a non-standard jar layout for classes
    val serviceProviderListPath = SERVICE_LOADER_PREFIX + classOf[SparkShimServiceProvider].getName
    val serviceProviderList = thisClassLoader.getResources(serviceProviderListPath)
        .asScala.map(scala.io.Source.fromURL)
        .flatMap(_.getLines())

    assert(serviceProviderList.nonEmpty, "Classpath should contain the resource for " +
        serviceProviderListPath)

    val shimServiceProviderOpt = serviceProviderList.flatMap { shimServiceProviderStr =>
      val mask = shimIdFromPackageName(shimServiceProviderStr)
      try {
        val shimURL = new java.net.URL(s"${shimRootURL.toString}$mask/")
        val shimClassLoader = new MutableURLClassLoader(Array(shimURL, shimCommonURL),
          thisClassLoader)
        // can't use ServiceLoader with parallel world layout
        val shimClass = shimClassLoader.loadClass(shimServiceProviderStr)
        Option(
          (instantiateClass(shimClass).asInstanceOf[SparkShimServiceProvider], shimURL)
        )
      } catch {
        case cnf: ClassNotFoundException =>
          logWarning(cnf + ": Could not load the provider", cnf)
          None
      }
    }.find { case (shimServiceProvider, _) =>
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

  // shimId corresponds to spark.version.classifier by convention
  // e.g. com.nvidia.spark.rapids.shims.spark320.SparkShimServiceProvider implies
  // shimId = "spark320"
  private def shimIdFromPackageName(shimServiceProvider: SparkShimServiceProvider) = {
    shimServiceProvider.getClass.getPackage.toString.split('.').last
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

  // TODO broken right now, check if this can be supported with parallel worlds
  // it implies the prerequisite of having such a class in the conventional root jar entry
  // - or the necessity of an additional parameter for specifying the shim subdirectory
  // - or enforcing the convention the class file parent directory is the shimid that is also
  //   a top entry e.g. /spark301/com/nvidia/test/shim/spark301/Spark301Shims.class
  def setSparkShimProviderClass(classname: String): Unit = {
    shimProviderClass = classname
  }

  def newInstanceOf[T](className: String): T = {
    val loader = getShimClassLoader()
    logDebug(s"Loading $className using $loader with the parent loader ${loader.getParent}")
    instantiateClass(loader.loadClass(className)).asInstanceOf[T]
  }

  def newInternalShuffleManager(conf: SparkConf, isDriver: Boolean): VisibleShuffleManager = {
    val shuffleClassLoader = getShimClassLoader()
    val shuffleClassName =
      s"org.apache.spark.sql.rapids.shims.${shimId}.RapidsShuffleInternalManager"
    val shuffleClass = shuffleClassLoader.loadClass(shuffleClassName)
    shuffleClass.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
        .asInstanceOf[VisibleShuffleManager]
  }

  // avoid cached constructors
  private def instantiateClass[T](cls: Class[T]): T = {
    logDebug(s"Instantiate ${cls.getName} using classloader " + cls.getClassLoader)
    cls.getClassLoader match {
      case m: MutableURLClassLoader =>
        logDebug("urls " + m.getURLs.mkString("\n"))
      case _ =>
    }
    val constructor = cls.getConstructor()
    constructor.newInstance()
  }
}
