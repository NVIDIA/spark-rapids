/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class NonDeterministicRetrySuite extends RmmSparkRetrySuiteBase {
  private val NUM_ROWS = 500
  private val RAND_SEED = 10

  private def buildBatch(ints: Seq[Int] = 0 until NUM_ROWS): ColumnarBatch = {
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), ints.length)
  }

  test("GPU rand outputs the same sequence with checkpoint and restore") {
    val gpuRand = GpuRand(GpuLiteral(RAND_SEED, IntegerType))
    withResource(buildBatch()) { inputCB =>
      // checkpoint the state
      gpuRand.checkpoint()
      val randHCol1 = withResource(gpuRand.columnarEval(inputCB)) { randCol1 =>
        randCol1.copyToHost()
      }
      withResource(randHCol1) { _ =>
        // store the state, and generate data again
        gpuRand.restore()
        val randHCol2 = withResource(gpuRand.columnarEval(inputCB)) { randCol2 =>
          randCol2.copyToHost()
        }
        withResource(randHCol2) { _ =>
          // check the two random columns are equal.
          assert(randHCol1.getRowCount.toInt == NUM_ROWS)
          assert(randHCol1.getRowCount == randHCol2.getRowCount)
          (0 until randHCol1.getRowCount.toInt).foreach { pos =>
            assert(randHCol1.getDouble(pos) == randHCol2.getDouble(pos))
          }
        }
      }
    }
  }

  test("GPU project retry with GPU rand") {
    val childOutput = Seq(AttributeReference("int", IntegerType)(NamedExpression.newExprId))
    val projectRandOnly = Seq(
      GpuAlias(GpuRand(GpuLiteral(RAND_SEED)), "rand")(NamedExpression.newExprId))
    val projectList = projectRandOnly ++ childOutput
    Seq(true, false).foreach { useTieredProject =>
      // expression should be retryable
      val randOnlyProjectList = GpuBindReferences.bindGpuReferencesTiered(projectRandOnly,
        childOutput, useTieredProject)
      assert(randOnlyProjectList.areAllRetryable)
      val boundProjectList = GpuBindReferences.bindGpuReferencesTiered(projectList,
        childOutput, useTieredProject)
      assert(boundProjectList.areAllRetryable)

      // project with retry
      val sb = closeOnExcept(buildBatch()) { cb =>
        SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      }
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId)
      withResource(boundProjectList.projectAndCloseWithRetrySingleBatch(sb)) { outCB =>
        // We can not verify the data, so only rows number here
        assertResult(NUM_ROWS)(outCB.numRows())
      }
    }
  }

}
