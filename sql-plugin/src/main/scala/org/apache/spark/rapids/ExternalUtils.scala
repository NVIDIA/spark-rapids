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

package org.apache.spark.rapids

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.{GpuOverrides, ScanMeta, ScanRule}

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.rapids.GpuAvroScan
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.util.Utils

object ExternalUtils {

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val loader = Utils.getContextOrSparkClassLoader

    /** spark-avro is an optional package for spark, so we should keep in mind that rapids can
     * run successfully even without it */
    Try(loader.loadClass("org.apache.spark.sql.v2.avro.AvroScan")) match {
      case Failure(_) => Map.empty
      case Success(_) => Seq(
        GpuOverrides.scan[AvroScan](
          "Avro parsing",
          (a, conf, p, r) => new ScanMeta[AvroScan](a, conf, p, r) {
            override def tagSelfForGpu(): Unit = GpuAvroScan.tagSupport(this)

            override def convertToGpu(): Scan =
              GpuAvroScan(a.sparkSession,
                a.fileIndex,
                a.dataSchema,
                a.readDataSchema,
                a.readPartitionSchema,
                a.options,
                conf,
                a.partitionFilters,
                a.dataFilters)
          })
      ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
    }
  }

}
