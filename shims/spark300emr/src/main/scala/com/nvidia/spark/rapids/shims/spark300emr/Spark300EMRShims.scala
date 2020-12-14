/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark300emr

import java.lang.reflect.Constructor

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark300.Spark300Shims
import com.nvidia.spark.rapids.spark300emr.RapidsShuffleManager

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}

class Spark300EMRShims extends Spark300Shims {

  private var fileScanRddConstructor: Option[Constructor[_]] = None

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  // use reflection here so we don't have to compile against their jars
  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition]): RDD[InternalRow] = {

    val constructor = fileScanRddConstructor.getOrElse {
      val tclass = classOf[org.apache.spark.sql.execution.datasources.FileScanRDD]
      val constructors = tclass.getConstructors()
      if (constructors.size > 1) {
        throw new IllegalStateException(s"Only expected 1 constructor for FileScanRDD")
      }
      val cnstr = constructors(0)
      fileScanRddConstructor = Some(cnstr)
      cnstr
    }
    val instance = if (constructor.getParameterCount() == 4) {
      constructor.newInstance(sparkSession, readFunction, filePartitions, None)
    } else if (constructor.getParameterCount() == 3) {
      constructor.newInstance(sparkSession, readFunction, filePartitions)
    } else {
      throw new IllegalStateException("Could not find appropriate constructor for FileScan RDD")
    }
    instance.asInstanceOf[FileScanRDD]
  }
}
