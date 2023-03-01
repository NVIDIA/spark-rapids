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
 * It reads from `/opt/alluxio/conf/alluxio-site.properties`
 */
class AlluxioConfigReader {

  def readAlluxioMasterAndPort(conf: RapidsConf): (String, String) = {
    readMasterAndPort(conf.getAlluxioHome)
  }

  // By default, read from /opt/alluxio, refer to `spark.rapids.alluxio.home` config in `RapidsConf`
  // The default port is 19998
  private[rapids] def readMasterAndPort(homePath: String): (String, String) = {
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
        throw new RuntimeException(s"Alluxio config file not found in " +
          s"$homePath/conf/alluxio-site.properties, " +
          "the default value of `spark.rapids.alluxio.home` is /opt/alluxio, " +
          "please create a link `/opt/alluxio` to Alluxio installation home, " +
          "or set `spark.rapids.alluxio.home` to Alluxio installation home")
    } finally {
      if (buffered_source != null) buffered_source.close
    }
  }
}
