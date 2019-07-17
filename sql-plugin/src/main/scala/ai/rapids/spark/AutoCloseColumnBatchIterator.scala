/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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
package ai.rapids.spark

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * It is the responsibility of the SparkPlan exec that creates a ColumnarBatch to close it,
 * This provides a simple way to be sure that it gets closed in all cases once it is done being
 * used.
 */
class AutoCloseColumnBatchIterator[U](itr: Iterator[U], nextBatch: Iterator[U] => ColumnarBatch)
    extends Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
    closeCurrentBatch()
  })

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = nextBatch(itr)
    cb
  }
}

object AutoCloseColumnBatchIterator {
  def map[U](rdd: RDD[U], f: (U) => ColumnarBatch) : RDD[ColumnarBatch] = {
    rdd.mapPartitions((itr) => new AutoCloseColumnBatchIterator(itr,
      (batchIter: Iterator[U]) => f(batchIter.next())))
  }
}
