/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import org.apache.spark.sql.execution.datasources.v2.{GpuInstructionExec, GpuMergeRowsExec, MergeRowsExec}

/**
 * Meta class for MergeRowsExec.
 * 
 * MergeRowsExec is a regular physical plan executor (like ProjectExec, FilterExec)
 * that processes merge row logic - it's not a table-specific V2 command.
 */
class GpuMergeRowsExecMeta(
    mergeRows: MergeRowsExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[MergeRowsExec](mergeRows, conf, p, rule) {

  // Extract all expressions from merge instructions
  override val childExprs: Seq[BaseExprMeta[_]] = {
    val isSourcePresent = GpuOverrides.wrapExpr(mergeRows.isSourceRowPresent, conf, Some(this))
    val isTargetPresent = GpuOverrides.wrapExpr(mergeRows.isTargetRowPresent, conf, Some(this))
    
    val matchedExprs = mergeRows.matchedInstructions.flatMap { instruction =>
      GpuOverrides.wrapExpr(instruction.condition, conf, Some(this)) +: 
        instruction.outputs.flatten.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
    }
    
    val notMatchedExprs = mergeRows.notMatchedInstructions.flatMap { instruction =>
      GpuOverrides.wrapExpr(instruction.condition, conf, Some(this)) +: 
        instruction.outputs.flatten.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
    }
    
    val notMatchedBySourceExprs = mergeRows.notMatchedBySourceInstructions.flatMap { instruction =>
      GpuOverrides.wrapExpr(instruction.condition, conf, Some(this)) +: 
        instruction.outputs.flatten.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
    }
    
    Seq(isSourcePresent, isTargetPresent) ++
      matchedExprs ++
      notMatchedExprs ++
      notMatchedBySourceExprs
  }

  override def convertToGpu(): GpuExec = {
    val gpuChild = childPlans.head.convertIfNeeded()
    
    // Convert presence expressions
    val gpuIsSourceRowPresent = childExprs(0).convertToGpu().asInstanceOf[GpuExpression]
    val gpuIsTargetRowPresent = childExprs(1).convertToGpu().asInstanceOf[GpuExpression]
    
    // Convert instruction sets to GpuInstructionExec instances
    var exprIdx = 2
    
    val gpuMatchedInstructions = mergeRows.matchedInstructions.map { instruction =>
      val gpuCond = childExprs(exprIdx).convertToGpu().asInstanceOf[GpuExpression]
      exprIdx += 1
      val gpuOutputs = instruction.outputs.map { output =>
        output.map { _ =>
          val gpuOutput = childExprs(exprIdx).convertToGpu().asInstanceOf[GpuExpression]
          exprIdx += 1
          gpuOutput
        }
      }
      new GpuInstructionExec(gpuCond, gpuOutputs, gpuChild.output)
    }
    
    val gpuNotMatchedInstructions = mergeRows.notMatchedInstructions.map { instruction =>
      val gpuCond = childExprs(exprIdx).convertToGpu().asInstanceOf[GpuExpression]
      exprIdx += 1
      val gpuOutputs = instruction.outputs.map { output =>
        output.map { _ =>
          val gpuOutput = childExprs(exprIdx).convertToGpu().asInstanceOf[GpuExpression]
          exprIdx += 1
          gpuOutput
        }
      }
      new GpuInstructionExec(gpuCond, gpuOutputs, gpuChild.output)
    }
    
    val gpuNotMatchedBySourceInstructions =
      mergeRows.notMatchedBySourceInstructions
      .map { instruction =>
      val gpuCond = childExprs(exprIdx).convertToGpu().asInstanceOf[GpuExpression]
      exprIdx += 1
      val gpuOutputs = instruction.outputs.map { output =>
        output.map { _ =>
          val gpuOutput = childExprs(exprIdx).convertToGpu().asInstanceOf[GpuExpression]
          exprIdx += 1
          gpuOutput
        }
      }
      new GpuInstructionExec(gpuCond, gpuOutputs, gpuChild.output)
    }
    
    GpuMergeRowsExec(
      gpuIsSourceRowPresent,
      gpuIsTargetRowPresent,
      gpuMatchedInstructions,
      gpuNotMatchedInstructions,
      gpuNotMatchedBySourceInstructions,
      mergeRows.checkCardinality,
      mergeRows.output,
      gpuChild)
  }
}

