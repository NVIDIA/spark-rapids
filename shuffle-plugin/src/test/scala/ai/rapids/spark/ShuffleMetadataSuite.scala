/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BufferType, ColumnVector, DType, Table}
import ai.rapids.spark.format.ColumnMeta
import org.scalatest.FunSuite

class ShuffleMetadataSuite extends FunSuite {

  test ("can create shuffle metadata for strings without nulls") {
    val cv = ColumnVector.fromStrings("hello", "strings", "foo")
    println(cv.getType)
    val table = new Table(cv)
    val meta = ShuffleMetadata.materializeResponse(ShuffleMetadata.getShuffleMetadataResponse(true, Seq(table).toArray, 0))

    assert(meta.tableMetaLength() == 1)
    assert(meta.tableMeta(0).columnMetasLength() == 1)
    val colMeta: ColumnMeta = meta.tableMeta(0).columnMetas(0)
    assert(DType.values()(colMeta.dType()) == DType.STRING)
    assert(colMeta.data().len() == cv.getDeviceBufferFor(BufferType.DATA).getLength)
    assert(colMeta.offsets().len() == cv.getDeviceBufferFor(BufferType.OFFSET).getLength)
    assert(colMeta.validity() == null)
  }

  test ("can create shuffle metadata for strings with nulls") {
    val cv = ColumnVector.fromStrings("hello", "strings", null, "foo")
    println(cv.getType)
    val table = new Table(cv)
    val meta = ShuffleMetadata.materializeResponse(ShuffleMetadata.getShuffleMetadataResponse(true, Seq(table).toArray, 0))

    assert(meta.tableMetaLength() == 1)
    assert(meta.tableMeta(0).columnMetasLength() == 1)
    val colMeta: ColumnMeta = meta.tableMeta(0).columnMetas(0)
    assert(DType.values()(colMeta.dType()) == DType.STRING)
    assert(colMeta.data().len() == cv.getDeviceBufferFor(BufferType.DATA).getLength)
    assert(colMeta.offsets().len() == cv.getDeviceBufferFor(BufferType.OFFSET).getLength)
    assert(colMeta.validity().len() == cv.getDeviceBufferFor(BufferType.VALIDITY).getLength)
  }
}
