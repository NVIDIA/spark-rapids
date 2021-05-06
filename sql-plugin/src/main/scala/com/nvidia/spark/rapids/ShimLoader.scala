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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}
import org.apache.spark.internal.Logging

object ShimLoader extends Logging {
  private var shimProviderClass: String = null
  private var sparkShims: SparkShims = null

  private def detectShimProvider(): SparkShimServiceProvider = {
    val sparkVersion = getSparkVersion
    logInfo(s"Loading shim for Spark version: $sparkVersion")

    // This is not ideal, but pass the version in here because otherwise loader that match the
    // same version (3.0.1 Apache and 3.0.1 Databricks) would need to know how to differentiate.
    val sparkShimLoaders = ServiceLoader.load(classOf[SparkShimServiceProvider])
        .asScala.filter(_.matchesVersion(sparkVersion))
    if (sparkShimLoaders.size > 1) {
      throw new IllegalArgumentException(s"Multiple Spark Shim Loaders found: $sparkShimLoaders")
    }
    logInfo(s"Found shims: $sparkShimLoaders")
    val loader = sparkShimLoaders.headOption match {
      case Some(loader) => loader
      case None =>
        throw new IllegalArgumentException(s"Could not find Spark Shim Loader for $sparkVersion")
    }
    loader
  }

  private def findShimProvider(): SparkShimServiceProvider = {
    if (shimProviderClass == null) {
      detectShimProvider()
    } else {
      logWarning(s"Overriding Spark shims provider to $shimProviderClass. " +
          "This may be an untested configuration!")
      val providerClass = Class.forName(shimProviderClass)
      val constructor = providerClass.getConstructor()
      constructor.newInstance().asInstanceOf[SparkShimServiceProvider]
    }
  }

  def getSparkShims: SparkShims = {
    if (sparkShims == null) {
      val provider = findShimProvider()
      sparkShims = provider.buildShim
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

  def setSparkShimProviderClass(classname: String): Unit = {
    shimProviderClass = classname
  }
}
