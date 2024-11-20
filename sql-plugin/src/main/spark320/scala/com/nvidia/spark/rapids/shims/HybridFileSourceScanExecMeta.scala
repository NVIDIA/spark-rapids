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

import com.nvidia.spark.rapids._

import org.apache.spark.rapids.hybrid.HybridFileSourceScanExec
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{BinaryType, MapType}

class HybridFileSourceScanExecMeta(plan: FileSourceScanExec,
                                   conf: RapidsConf,
                                   parent: Option[RapidsMeta[_, _, _]],
                                   rule: DataFromReplacementRule)
  extends SparkPlanMeta[FileSourceScanExec](plan, conf, parent, rule) {

  // Replaces SubqueryBroadcastExec inside dynamic pruning filters with native counterpart
  // if possible. Instead regarding filters as childExprs of current Meta, we create
  // a new meta for SubqueryBroadcastExec. The reason is that the native replacement of
  // FileSourceScan is independent from the replacement of the partitionFilters.
  private lazy val partitionFilters = {
    val convertBroadcast = (bc: SubqueryBroadcastExec) => {
      val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
      meta.tagForExplain()
      meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
    }
    wrapped.partitionFilters.map { filter =>
      filter.transformDown {
        case dpe@DynamicPruningExpression(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
            case reuse@ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
            case _ =>
              dpe
          }
      }
    }
  }

  // partition filters and data filters are not run on the GPU
  override val childExprs: Seq[ExprMeta[_]] = Seq.empty

  override def tagPlanForGpu(): Unit = {
    val cls = wrapped.relation.fileFormat.getClass
    if (cls != classOf[ParquetFileFormat]) {
      willNotWorkOnGpu(s"unsupported file format: ${cls.getCanonicalName}")
    }
  }

  override def convertToGpu(): GpuExec = {
    // Modifies the original plan to support DPP
    val fixedExec = wrapped.copy(partitionFilters = partitionFilters)
    HybridFileSourceScanExec(fixedExec)(conf)
  }
}

object HybridFileSourceScanExecMeta {

  // Determines whether using HybridScan or GpuScan
  def useHybridScan(conf: RapidsConf, fsse: FileSourceScanExec): Boolean = {
    val isEnabled = if (conf.useHybridParquetReader) {
      require(conf.loadHybridBackend,
        "Hybrid backend was NOT loaded during the launch of spark-rapids plugin")
      true
    } else {
      false
    }
    // For the time being, only support reading Parquet
    lazy val isParquet = fsse.relation.fileFormat.getClass == classOf[ParquetFileFormat]
    // Check if data types of all fields are supported by HybridParquetReader
    lazy val allSupportedTypes = fsse.requiredSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, {
        // For the time being, the native backend may return incorrect results over nestedMap
        case MapType(kt, vt, _) if kt.isInstanceOf[MapType] || vt.isInstanceOf[MapType] => false
        // For the time being, BinaryType is not supported yet
        case _: BinaryType => false
        case _ => true
      })
    }
    // TODO: supports BucketedScan
    lazy val noBucketedScan = !fsse.bucketedScan
    // TODO: supports working along with Alluxio
    lazy val noAlluxio = !AlluxioCfgUtils.enabledAlluxioReplacementAlgoConvertTime(conf)

    isEnabled && isParquet && allSupportedTypes && noBucketedScan && noAlluxio
  }

}
