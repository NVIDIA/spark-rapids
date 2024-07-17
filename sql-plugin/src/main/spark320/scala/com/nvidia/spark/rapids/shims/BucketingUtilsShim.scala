/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.RapidsMeta

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.rapids.{BucketIdMetaUtils, GpuWriterBucketSpec}

object BucketingUtilsShim {

  def getWriterBucketSpec(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute],
      options: Map[String, String],
      forceHiveHash: Boolean): Option[GpuWriterBucketSpec] = {
    bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
      if (forceHiveHash) {
        // Forcely use HiveHash for Hive write commands for some customized Spark binaries.
        BucketIdMetaUtils.getWriteBucketSpecForHive(bucketColumns, spec.numBuckets)
      } else {
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression = GpuHashPartitioning(bucketColumns, spec.numBuckets)
          .partitionIdExpression
        GpuWriterBucketSpec(bucketIdExpression, (_: Int) => "")
      }
    }
  }

  def isHiveHashBucketing(options: Map[String, String]): Boolean = false

  def getOptionsWithHiveBucketWrite(bucketSpec: Option[BucketSpec]): Map[String, String] = {
    Map.empty
  }

  def tagForHiveBucketingWrite(meta: RapidsMeta[_, _, _], bucketSpec: Option[BucketSpec],
      outColumns: Seq[Attribute], forceHiveHash: Boolean): Unit = {
    if (forceHiveHash) {
      BucketIdMetaUtils.tagForBucketingHiveWrite(meta, bucketSpec, outColumns)
    } else {
      BucketIdMetaUtils.tagForBucketingWrite(meta, bucketSpec, outColumns)
    }
  }
}
