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

import ai.rapids.cudf.{ColumnVector, DType, Table}
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType}
import java.util
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuGenerateSuite
  extends SparkQueryCompareTestSuite
    with Arm {
  val rapidsConf = new RapidsConf(Map.empty[String, String])

  def makeListColumn(
      numRows: Int,
      listSize: Int,
      includeNulls: Boolean,
      allNulls: Boolean): ColumnVector = {
    val list = util.Arrays.asList((0 until listSize): _*)
    val rows = (0 until numRows).map { r =>
      if (allNulls || includeNulls && r % 2 == 0) {
        null
      } else {
        list
      }
    }
    ColumnVector.fromLists(
      new ListType(true,
        new BasicType(true, DType.INT32)),
      rows: _*)
  }

  def makeBatch(
      numRows: Int,
      includeRepeatColumn: Boolean = true,
      includeNulls: Boolean = false,
      listSize: Int = 4,
      allNulls: Boolean = false): (ColumnarBatch, Long) = {
    var inputSize: Long = 0L
    withResource(makeListColumn(numRows, listSize, includeNulls, allNulls)) { cvList =>
      inputSize +=
        withResource(cvList.getChildColumnView(0)) {
          _.getDeviceMemorySize
        }
      val batch = if (includeRepeatColumn) {
        val dt: Array[DataType] = Seq(IntegerType, ArrayType(IntegerType)).toArray
        val secondColumn = (0 until numRows).map { x =>
          val i = Int.box(x)
          if (allNulls || includeNulls && i % 2 == 0) {
            null
          } else {
            i
          }
        }
        withResource(ColumnVector.fromBoxedInts(secondColumn: _*)) { repeatCol =>
          inputSize += listSize * repeatCol.getDeviceMemorySize
          withResource(new Table(repeatCol, cvList)) { tbl =>
            GpuColumnVector.from(tbl, dt)
          }
        }
      } else {
        val dt: Array[DataType] = Seq(ArrayType(IntegerType)).toArray
        withResource(new Table(cvList)) { tbl =>
          GpuColumnVector.from(tbl, dt)
        }
      }
      (batch, inputSize)
    }
  }

  def checkSplits(splits: Array[Int], batch: ColumnarBatch): Unit = {
    withResource(GpuColumnVector.from(batch)) { tbl =>
      var totalRows = 0L
      // because concatenate does not work with 1 Table and I can't incRefCount a Table
      // this is used to close the concatenated table, which would otherwise be leaked.
      withResource(new ArrayBuffer[Table]) { tableToClose =>
        withResource(tbl.contiguousSplit(splits: _*)) { splitted =>
          splitted.foreach { ct =>
            totalRows += ct.getRowCount
          }
          // `getTable` causes Table to be owned by the `ContiguousTable` class
          // so they get closed when the `ContiguousTable`s get closed.
          val concatted = if (splitted.length == 1) {
            splitted(0).getTable
          } else {
            val tbl = Table.concatenate(splitted.map(_.getTable): _*)
            tableToClose += tbl
            tbl
          }
          // Compare row by row the input vs the concatenated splits
          withResource(GpuColumnVector.from(batch)) { inputTbl =>
            assertResult(concatted.getRowCount)(batch.numRows())
            (0 until batch.numCols()).foreach { c =>
              withResource(concatted.getColumn(c).copyToHost()) { hostConcatCol =>
                withResource(inputTbl.getColumn(c).copyToHost()) { hostInputCol =>
                  (0 until batch.numRows()).foreach { r =>
                    if (hostInputCol.isNull(r)) {
                      assertResult(true)(hostConcatCol.isNull(r))
                    } else {
                      if (hostInputCol.getType == DType.LIST) {
                        // exploding column
                        compare(hostInputCol.getList(r), hostConcatCol.getList(r))
                      } else {
                        compare(hostInputCol.getInt(r), hostConcatCol.getInt(r))
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("all null inputs") {
    val (batch, _) = makeBatch(numRows = 100, allNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      assertResult(0)(
        e.inputSplitIndices(batch, 1, false, 4).length)

      // Here we are going to split 99 times, since our targetSize is 1 Byte and we are going to
      // produce 100 rows. The row byte count for a null row is going to be sizeof(type), and
      // because we use `outer=true` we are expecting 4 bytes x 100 rows.
      assertResult(99)(
        e.inputSplitIndices(batch, generatorOffset = 1 , true, 4).length)
    }
  }

  test("0-row batches short-circuits to no splits") {
    val (batch, _) = makeBatch(numRows = 0)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      assertResult(0)(
        e.inputSplitIndices(batch, 1, false, 1).length)
      assertResult(0)(
        e.inputSplitIndices(batch, generatorOffset = 1 , true, 1).length)
    }
  }

  test("1-row batches short-circuits to no splits") {
    val (batch, _) = makeBatch(numRows = 1)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      var splits = e.inputSplitIndices(batch, 1, false, 1)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, generatorOffset = 1 , true, 1)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)
    }
  }

  test("2-row batches split in half") {
    val (batch, inputSize) = makeBatch(numRows = 2)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val target = inputSize/2
      var splits = e.inputSplitIndices(batch, 1, false, target)
      assertResult(1)(splits.length)
      assertResult(1)(splits(0))
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, generatorOffset = 1 , true, target)
      assertResult(1)(splits.length)
      assertResult(1)(splits(0))
      checkSplits(splits, batch)
    }
  }

  test("8-row batch splits in half") {
    val (batch, inputSize) = makeBatch(numRows = 8)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val target = inputSize/2
      // here a split at 4 actually means produce two Tables, each with 4 rows.
      var splits = e.inputSplitIndices(batch, 1, false, target)
      assertResult(1)(splits.length)
      assertResult(4)(splits(0))
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, generatorOffset = 1 , true, target)
      assertResult(1)(splits.length)
      assertResult(4)(splits(0))
      checkSplits(splits, batch)
    }
  }

  // these next four tests exercise code that just uses the exploding column's size as the limit
  test("test batch with a single exploding column") {
    val (batch, _) = makeBatch(numRows = 100, includeRepeatColumn = false)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      val targetSize = 1600
      // 1600 == a single split
      var splits = e.inputSplitIndices(batch, 0, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 800 == 1 splits (2 parts) right down the middle
      splits = e.inputSplitIndices(batch, 0, false, targetSize/2)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))
      checkSplits(splits, batch)

      // 400 == 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 0, false, targetSize/4)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
      checkSplits(splits, batch)
    }
  }

  test("test batch with a single exploding column with nulls") {
    val (batch, inputSize) = makeBatch(numRows=100, includeRepeatColumn=false, includeNulls=true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // with nulls it should be 1/2 that
      val targetSize = inputSize
      // 800 = no splits
      var splits = e.inputSplitIndices(batch, 0, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 400 == 1 splits (2 parts) right down the middle
      splits = e.inputSplitIndices(batch, 0, false, targetSize/2)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))
      checkSplits(splits, batch)

      // 200 == 8 parts
      splits = e.inputSplitIndices(batch, 0, false, targetSize/4)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
      checkSplits(splits, batch)
    }
  }

  test("outer: test batch with a single exploding column with nulls") {
    // for outer, size is the same (nulls are assumed not to occupy any space, which is not true)
    // but number of rows is not the same.
    val (batch, inputSize) = makeBatch(numRows=100, includeRepeatColumn=false, includeNulls=true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // with nulls it should be 1/2 that
      val targetSize = inputSize
      // 800 = no splits
      assertResult(0)(
        e.inputSplitIndices(batch, 0, true, targetSize).length)

      // 400 == 1 splits (2 parts) right down the middle
      var splits = e.inputSplitIndices(batch, 0, true, targetSize/2)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))

      // 200 == 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 0, true, targetSize/4)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
    }
  }

  test("outer, limit rows: test batch with a single exploding column with nulls") {
    // for outer, size is the same (nulls are assumed not to occupy any space, which is not true)
    // but number of rows is not the same.
    val (batch, inputSize) = makeBatch(numRows=100, includeRepeatColumn=false, includeNulls=true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // with nulls it should be 1/2 that
      // 250 rows is the expected number of rows in this case:
      //   200 non-null rows (4 reps * 50 non-null) +
      //   50 for rows that had nulls, since those are produced as well.
      // no-splits
      assertResult(0)(
        e.inputSplitIndices(batch, 0, true, inputSize, maxRows = 250).length)

      // 1 split (2 parts)
      var splits = e.inputSplitIndices(batch, 0, true, inputSize, maxRows = 125)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))

      // 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 0, true, inputSize, maxRows = 63)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
    }
  }

  test("test batch with a repeating column") {
    val (batch, inputSize) = makeBatch(numRows=100)
    withResource(batch) { _ =>
      val e = GpuExplode(null)

      // no splits
      var targetSize = inputSize //3200 Bytes
      var splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 1600 == 1 splits (2 parts) right down the middle
      targetSize = inputSize / 2
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(1)(splits.length)
      assertResult(50)(splits(0))
      checkSplits(splits, batch)

      targetSize = inputSize / 4
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(3)(splits.length)
      assertResult(25)(splits(0))
      assertResult(50)(splits(1))
      assertResult(75)(splits(2))
      checkSplits(splits, batch)

      targetSize = inputSize / 8
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(7)(splits.length)
      assertResult(13)(splits(0))
      assertResult(25)(splits(1))
      assertResult(38)(splits(2))
      assertResult(50)(splits(3))
      assertResult(63)(splits(4))
      assertResult(75)(splits(5))
      assertResult(88)(splits(6))
      checkSplits(splits, batch)
    }
  }

  test("test batch with a repeating column with nulls") {
    val (batch, _) = makeBatch(numRows=100, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      val inputSize = 1600
      var targetSize = inputSize
      var splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      // 800 == 1 splits (2 parts) right down the middle
      targetSize = inputSize / 2
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(1)(splits.length)
      checkSplits(splits, batch)

      // 400 == 3 splits (4 parts)
      targetSize = inputSize / 4
      splits = e.inputSplitIndices(batch, 1, false, targetSize)
      assertResult(3)(splits.length)
      checkSplits(splits, batch)

      // we estimate 1600 bytes in this scenario (1000 for repeating col, and 800 for exploding)
      // in this case we use 32 bytes as the target size, so that is ceil(1800/32) => 57 splits.
      splits = e.inputSplitIndices(batch, 1, false, 32)
      assertResult(49)(splits.length)
      checkSplits(splits, batch)

      // in this case we use 16 bytes as the target size, resulting in 1600/8 > 100 splits,
      // which is more than the number of input rows, so we fallback to splitting at most to
      // input number of rows 100. Since the input is half nulls, then we get 50 splits.
      splits = e.inputSplitIndices(batch, 1, false, 8)
      assertResult(50)(splits.length)
      checkSplits(splits, batch)
    }
  }

  test("outer: test batch with a repeating column with nulls") {
    val (batch, inputSize) = makeBatch(numRows = 100, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // the repeating column is 4 bytes (or 400 bytes total) repeated 4 times (or 1600)
      // 1600 == two splits
      var targetSize = inputSize // 2656 Bytes
      var splits = e.inputSplitIndices(batch, 1, true, targetSize)
      checkSplits(splits, batch)

      // 1600 == 1 splits (2 parts) right down the middle
      targetSize = inputSize / 2
      splits = e.inputSplitIndices(batch, 1, true, targetSize)
      checkSplits(splits, batch)

      // 800 == 3 splits (4 parts)
      targetSize = inputSize / 4
      splits = e.inputSplitIndices(batch, 1, true, targetSize)
      checkSplits(splits, batch)

      // we estimate 1800 bytes in this scenario (1000 for repeating col, and 800 for exploding)
      // this is slightly more splits from the outer=false case, since we weren't counting nulls
      // then.
      splits = e.inputSplitIndices(batch, 1, true, 32)
      assertResult(55)(splits.length)
      checkSplits(splits, batch)

      // in this case we use 16 bytes as the target size, resulting in 1800/16 > 100 splits,
      // which is more than the number of input rows, so we fallback to splitting at most to
      // input number of rows 100. Since the input is half nulls, then we get 50 splits.
      splits = e.inputSplitIndices(batch, 1, true, 16)
      assertResult(50)(splits.length)
      checkSplits(splits, batch)
    }
  }

  test("outer: test 1000 row batch with a repeating column with nulls") {
    val (batch, _) = makeBatch(numRows = 10000, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // the exploded column should be 4 Bytes * 100 rows * 4 reps per row = 1600 Bytes.
      // the repeating column is 4 bytes (or 400 bytes total) repeated 4 times (or 1600)
      // 1600 == two splits
      var splits = e.inputSplitIndices(batch, 1, true, 1600)
      checkSplits(splits, batch)

      // 1600 == 1 splits (2 parts) right down the middle
      splits = e.inputSplitIndices(batch, 1, true, 800)
      checkSplits(splits, batch)

      // 800 == 3 splits (4 parts)
      splits = e.inputSplitIndices(batch, 1, true, 400)
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, 1, true, 100)
      checkSplits(splits, batch)
    }
  }

  test("if the row limit produces more splits, prefer splitting using maxRows") {
    val (batch, inputSize) = makeBatch(numRows = 10000, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // by size try to no splits, instead we should get 2 (by maxRows)
      // we expect 40000 rows (4x1000 given 4 items in the list per row), but we included nulls,
      // so this should return 20000 rows (given that this is not outer)
      var splits = e.inputSplitIndices(batch, 1, false, inputSize, maxRows = 20000)
      assertResult(0)(splits.length)
      checkSplits(splits, batch)

      splits = e.inputSplitIndices(batch, 1, false, inputSize, maxRows = 10000)
      assertResult(1)(splits.length)
      assertResult(5000)(splits(0))
      checkSplits(splits, batch)
    }
  }

  test("outer: if the row limit produces more splits, prefer splitting using maxRows") {
    val (batch, inputSize) = makeBatch(numRows = 10000, includeNulls = true)
    withResource(batch) { _ =>
      val e = GpuExplode(null)
      // by size try to no splits, instead we should get 2 (by maxRows)
      // we expect 40000 rows (4x1000 given 4 items in the list per row)
      val splits = e.inputSplitIndices(batch, 1, true, inputSize, maxRows = 20000)
      assertResult(1)(splits.length)
      assertResult(5000)(splits(0))
      checkSplits(splits, batch)
    }
  }
}
