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
package com.nvidia.spark.rapids

import java.io.FileNotFoundException
import java.util.Properties

import scala.io.BufferedSource

/**
 * Alluxio master address and port reader.
 * It reads from `/opt/alluxio-2.8.0/conf/alluxio-site.properties`
 */
class AlluxioMasterAndPortReader {

  private val alluxioHome: String = "/opt/alluxio-2.8.0"

  def readAlluxioMasterAndPort(): (String, String) = {
    readAlluxioMasterAndPort(alluxioHome)
  }

  // Default to read from /opt/alluxio-2.8.0 if not setting ALLUXIO_HOME
  private[rapids] def readAlluxioMasterAndPort(defaultHomePath: String): (String, String) = {
    val homePath = scala.util.Properties.envOrElse("ALLUXIO_HOME", defaultHomePath)

    var buffered_source: BufferedSource = null
    try {
      buffered_source = scala.io.Source.fromFile(homePath + "/conf/alluxio-site.properties")
      val prop: Properties = new Properties()
      prop.load(buffered_source.bufferedReader())
      val alluxio_master = prop.getProperty("alluxio.master.hostname")
      if (alluxio_master == null) {
        throw new RuntimeException(
          s"Can't find alluxio.master.hostname from $homePath/conf/alluxio-site.properties.")
      }

      val alluxio_port = prop.getProperty("alluxio.master.rpc.port", "19998")
      (alluxio_master, alluxio_port)
    } catch {
      case _: FileNotFoundException =>
        throw new RuntimeException(s"Not found Alluxio config in " +
          s"$homePath/conf/alluxio-site.properties, " +
          "please check if ALLUXIO_HOME is set correctly")
    } finally {
      if (buffered_source != null) buffered_source.close
    }
  }
}
