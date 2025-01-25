/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.spill

import java.io.File
import java.math.RoundingMode

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.CodecType
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class SpillFrameworkSuite
  extends FunSuiteWithTempDir
    with MockitoSugar
    with BeforeAndAfterAll {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val sc = new SparkConf
    sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1024")
    SpillFramework.initialize(new RapidsConf(sc))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SpillFramework.shutdown()
  }

  private def buildContiguousTable(): (ContiguousTable, Array[DataType]) = {
    val (tbl, dataTypes) = buildTable()
    withResource(tbl) { _ =>
      (tbl.contiguousSplit()(0), dataTypes)
    }
  }

  private def buildTableOfLongs(numRows: Int): (ContiguousTable, Array[DataType])= {
    val vals = (0 until numRows).map(_.toLong)
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        withResource(new Table(cv)) { table =>
          (table.contiguousSplit()(0), Array[DataType](LongType))
        }
      }
    }
  }

  private def buildNonContiguousTableOfLongs(
      numRows: Int): (Table, Array[DataType])= {
    val vals = (0 until numRows).map(_.toLong)
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        (new Table(cv), Array[DataType](LongType))
      }
    }
  }

  private def buildTable(): (Table, Array[DataType]) = {
    val tbl = new Table.TestBuilder()
      .column(5, null.asInstanceOf[java.lang.Integer], 3, 1)
      .column("five", "two", null, null)
      .column(5.0, 2.0, 3.0, 1.0)
      .decimal64Column(-5, RoundingMode.UNNECESSARY, 0, null, -1.4, 10.123)
      .build()
    val types: Array[DataType] =
      Seq(IntegerType, StringType, DoubleType, DecimalType(10, 5)).toArray
    (tbl, types)
  }

  private def buildTableWithDuplicate(): (Table, Array[DataType]) = {
    withResource(ColumnVector.fromInts(5, null.asInstanceOf[java.lang.Integer], 3, 1)) { intCol =>
      withResource(ColumnVector.fromStrings("five", "two", null, null)) { stringCol =>
        withResource(ColumnVector.fromDoubles(5.0, 2.0, 3.0, 1.0)) { doubleCol =>
          // add intCol twice
          (new Table(intCol, intCol, stringCol, doubleCol),
            Array(IntegerType, IntegerType, StringType, DoubleType))
        }
      }
    }
  }

  private def buildEmptyTable(): (Table, Array[DataType]) = {
    val (tbl, types) = buildTable()
    val emptyTbl = withResource(tbl) { _ =>
      withResource(ColumnVector.fromBooleans(false, false, false, false)) { mask =>
        tbl.filter(mask) // filter all out
      }
    }
    (emptyTbl, types)
  }

  private def testBufferFileDeletion(canShareDiskPaths: Boolean): Unit = {
    val (_, handle, _) = addContiguousTableToFramework()
    var path: File = null
    withResource(handle) { _ =>
      SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
      SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes)
      assert(handle.host.isDefined)
      assert(handle.host.map(_.disk.isDefined).get)
      path = SpillFramework.stores.diskStore.getFile(handle.host.flatMap(_.disk).get.blockId)
      assert(path.exists)
    }
    assert(!path.exists)
  }

  private def addContiguousTableToFramework(): (
    Long, SpillableColumnarBatchFromBufferHandle, Array[DataType]) = {
    val (ct, dataTypes) = buildContiguousTable()
    val bufferSize = ct.getBuffer.getLength
    val handle = SpillableColumnarBatchFromBufferHandle(ct, dataTypes)
    (bufferSize, handle, dataTypes)
  }

  private def addTableToFramework(): (SpillableColumnarBatchHandle, Array[DataType]) = {
    // store takes ownership of the table
    val (tbl, dataTypes) = buildTable()
    val cb = withResource(tbl) { _ => GpuColumnVector.from(tbl, dataTypes) }
    val handle = SpillableColumnarBatchHandle(cb)
    (handle, dataTypes)
  }

  private def addZeroRowsTableToFramework(): (SpillableColumnarBatchHandle, Array[DataType]) = {
    val (table, dataTypes) = buildEmptyTable()
    val cb = withResource(table) { _ => GpuColumnVector.from(table, dataTypes) }
    val handle = SpillableColumnarBatchHandle(cb)
    (handle, dataTypes)
  }

  private def buildHostBatch(): (ColumnarBatch, Array[DataType]) = {
    val (ct, dataTypes) = buildContiguousTable()
    val hostCols = withResource(ct) { _ =>
      withResource(ct.getTable) { tbl =>
        (0 until tbl.getNumberOfColumns)
          .map(c => tbl.getColumn(c).copyToHost())
      }
    }.toArray
    (new ColumnarBatch(
      hostCols.zip(dataTypes).map { case (hostCol, dataType) =>
        new RapidsHostColumnVector(dataType, hostCol)
      }, hostCols.head.getRowCount.toInt), dataTypes)
  }

  private def buildHostBatchWithDuplicate(): (ColumnarBatch, Array[DataType]) = {
    val (ct, dataTypes) = buildContiguousTable()
    val hostCols = withResource(ct) { _ =>
      withResource(ct.getTable) { tbl =>
        (0 until tbl.getNumberOfColumns)
          .map(c => tbl.getColumn(c).copyToHost())
      }
    }.toArray
    hostCols.foreach(_.incRefCount())
    (new ColumnarBatch(
      (hostCols ++ hostCols).zip(dataTypes ++ dataTypes).map { case (hostCol, dataType) =>
        new RapidsHostColumnVector(dataType, hostCol)
      }, hostCols.head.getRowCount.toInt), dataTypes)
  }

  test("add table registers with device store") {
    val (ct, dataTypes) = buildContiguousTable()
    withResource(SpillableColumnarBatchFromBufferHandle(ct, dataTypes)) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("a non-contiguous table is spillable and it is handed over to the store") {
    val (tbl, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(tbl, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
    }
  }

  test("a non-contiguous table becomes non-spillable when batch is obtained") {
    val (tbl, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(tbl, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(handle.materialize(dataTypes)) { _ =>
        assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assertResult(0)(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
      }
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(handle.approxSizeInBytes)(
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
    }
  }

  test("a non-contiguous table is non-spillable until all columns are returned") {
    val (table, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(table, dataTypes)) { handle =>
      assert(handle.spillable)
      val cb = handle.materialize(dataTypes)
      assert(!handle.spillable)
      val columns = GpuColumnVector.extractBases(cb)
      withResource(columns.head) { _ =>
        columns.head.incRefCount()
        withResource(cb) { _ =>
          assert(!handle.spillable)
        }
        // still 0 after the batch is closed, because of the extra incRefCount
        // for columns.head
        assert(!handle.spillable)
      }
      // columns.head is closed, so now our RapidsTable is spillable again
      assert(handle.spillable)
    }
  }

  test("an aliased non-contiguous table is not spillable (until closing the alias) ") {
    val (table, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(table, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(SpillableColumnarBatchHandle(handle.materialize(dataTypes))) { aliasHandle =>
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      } // we now have two copies in the store
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("an aliased contiguous table is not spillable (until closing the alias) ") {
    val (table, dataTypes) = buildContiguousTable()
    withResource(SpillableColumnarBatchFromBufferHandle(table, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      val materialized = handle.materialize(dataTypes)
      // note that materialized is a batch "from buffer", it is not a regular batch
      withResource(SpillableColumnarBatchFromBufferHandle(materialized)) { aliasHandle =>
        // we now have two copies in the store
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      }
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("an non-contiguous table supports duplicated columns") {
    val (table, dataTypes) = buildTableWithDuplicate()
    withResource(SpillableColumnarBatchHandle(table, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(SpillableColumnarBatchHandle(handle.materialize(dataTypes))) { aliasHandle =>
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      } // we now have two copies in the store
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("a buffer is not spillable until the owner closes columns referencing it") {
    val (ct, _) = buildContiguousTable()
    // the contract for spillable handles is that they take ownership
    // incRefCount to follow that pattern
    val buff = ct.getBuffer
    buff.incRefCount()
    withResource(SpillableDeviceBufferHandle(buff)) { handle =>
      withResource(ct) { _ =>
        assert(!handle.spillable)
      }
      assert(handle.spillable)
    }
  }

  private def buildContiguousTable(start: Int, numRows: Int): ContiguousTable = {
    val vals = (0 until numRows).map(_.toLong + start)
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        withResource(HostColumnVector.decimalFromLongs(-3, vals: _*)) { decHcv =>
          withResource(decHcv.copyToDevice()) { decCv =>
            withResource(new Table(cv, decCv)) { table =>
              table.contiguousSplit()(0)
            }
          }
        }
      }
    }
  }

  private def buildCompressedBatch(start: Int, numRows: Int): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(
      CodecType.NVCOMP_LZ4, TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      compressor.addTableToCompress(buildContiguousTable(start, numRows))
      withResource(compressor.finish()) { compressed =>
        GpuCompressedColumnVector.from(compressed.head)
      }
    }
  }

  private def decompressBatch(cb: ColumnarBatch): ColumnarBatch = {
    val schema = new StructType().add("i", LongType)
      .add("j", DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 3))
    val sparkTypes = GpuColumnVector.extractTypes(schema)
    val codec = TableCompressionCodec.getCodec(
      CodecType.NVCOMP_LZ4, TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchDecompressor(0, Cuda.DEFAULT_STREAM)) { decompressor =>
      val gcv = cb.column(0).asInstanceOf[GpuCompressedColumnVector]
      // we need to incRefCount since the decompressor closes its inputs
      gcv.getTableBuffer.incRefCount()
      decompressor.addBufferToDecompress(gcv.getTableBuffer, gcv.getTableMeta.bufferMeta())
      withResource(decompressor.finishAsync()) { decompressed =>
        MetaUtils.getBatchFromMeta(
          decompressed.head,
          MetaUtils.dropCodecs(gcv.getTableMeta),
          sparkTypes)
      }
    }
  }

  test("a compressed batch can be added and recovered") {
    val ct = buildCompressedBatch(0, 1000)
    withResource(SpillableCompressedColumnarBatchHandle(ct)) { handle =>
      assert(handle.spillable)
      withResource(handle.materialize()) { materialized =>
        assert(!handle.spillable)
        // since we didn't spill, these buffers are exactly the same
        assert(
          ct.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer ==
            materialized.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer)
      }
      assert(handle.spillable)
    }
  }

  test("a compressed batch can be added and recovered after being spilled to host") {
    val ct = buildCompressedBatch(0, 1000)
    withResource(decompressBatch(ct)) { decompressedExpected =>
      withResource(SpillableCompressedColumnarBatchHandle(ct)) { handle =>
        assert(handle.spillable)
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        assert(!handle.spillable)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        withResource(handle.materialize()) { materialized =>
          withResource(decompressBatch(materialized)) { decompressed =>
            TestUtils.compareBatches(decompressedExpected, decompressed)
          }
        }
      }
    }
  }

  test("a compressed batch can be added and recovered after being spilled to disk") {
    val ct = buildCompressedBatch(0, 1000)
    withResource(decompressBatch(ct)) { decompressedExpected =>
      withResource(SpillableCompressedColumnarBatchHandle(ct)) { handle =>
        assert(handle.spillable)
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        assert(!handle.spillable)
        SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        assert(handle.host.get.host.isEmpty)
        assert(handle.host.get.disk.isDefined)
        withResource(handle.materialize()) { materialized =>
          withResource(decompressBatch(materialized)) { decompressed =>
            TestUtils.compareBatches(decompressedExpected, decompressed)
          }
        }
      }
    }
  }


  test("a second handle prevents buffer to be spilled") {
    val buffer = DeviceMemoryBuffer.allocate(123)
    val handle1 = SpillableDeviceBufferHandle(buffer)
    // materialize will incRefCount `buffer`. This looks a little weird
    // but it simulates aliasing as it happens in real code
    val handle2 = SpillableDeviceBufferHandle(handle1.materialize())

    withResource(handle1) { _ =>
      withResource(handle2) { _ =>
        assertResult(2)(handle1.dev.get.getRefCount)
        assertResult(2)(handle2.dev.get.getRefCount)
        assertResult(false)(handle1.spillable)
        assertResult(false)(handle2.spillable)
      }
      assertResult(1)(handle1.dev.get.getRefCount)
      assertResult(true)(handle1.spillable)
    }
  }

  test("removing handle releases buffer resources in all stores") {
    val handle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(123))
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(0)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)

      assertResult(123)(SpillFramework.stores.deviceStore.spill(123)) // spill to host memory
      assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)
      assert(handle.dev.isEmpty)
      assert(handle.host.isDefined)
      assert(handle.host.get.host.isDefined)

      assertResult(123)(SpillFramework.stores.hostStore.spill(123)) // spill to disk
      assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(0)(SpillFramework.stores.hostStore.numHandles)
      assertResult(1)(SpillFramework.stores.diskStore.numHandles)
      assert(handle.dev.isEmpty)
      assert(handle.host.isDefined)
      assert(handle.host.get.host.isEmpty)
      assert(handle.host.get.disk.isDefined)
    }
    assert(handle.host.isEmpty)
    assert(handle.dev.isEmpty)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    assertResult(0)(SpillFramework.stores.hostStore.numHandles)
    assertResult(0)(SpillFramework.stores.diskStore.numHandles)
  }

  test("spill updates store state") {
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore

    val (bufferSize, handle, _) =
      addContiguousTableToFramework()

    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)

      assertResult(bufferSize)(SpillFramework.stores.deviceStore.spill(bufferSize))
      assertResult(bufferSize)(SpillFramework.stores.hostStore.spill(bufferSize))

      assertResult(0)(deviceStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      assertResult(1)(diskStore.numHandles)

      val diskHandle = handle.host.flatMap(_.disk).get
      val path = diskStore.getFile(diskHandle.blockId)
      assert(path.exists)
    }
  }

  test("get columnar batch after host spill") {
    val (ct, dataTypes) = buildContiguousTable()
    val expectedBatch = GpuColumnVector.from(ct.getTable, dataTypes)
    withResource(SpillableColumnarBatchFromBufferHandle(
      ct, dataTypes)) { handle =>
      withResource(expectedBatch) { _ =>
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        withResource(handle.materialize(dataTypes)) { cb =>
          TestUtils.compareBatches(expectedBatch, cb)
        }
      }
    }
  }

  test("get memory buffer after host spill") {
    val (ct, dataTypes) = buildContiguousTable()
    val expectedBatch = closeOnExcept(ct) { _ =>
      // make a copy of the table so we can compare it later to the
      // one reconstituted after the spill
      withResource(ct.getTable.contiguousSplit()) { copied =>
        GpuColumnVector.from(copied(0).getTable, dataTypes)
      }
    }
    val handle = SpillableColumnarBatchFromBufferHandle(ct, dataTypes)
    withResource(handle) { _ =>
      withResource(expectedBatch) { _ =>
        assertResult(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))(
          handle.approxSizeInBytes)
        val hostSize = handle.host.get.approxSizeInBytes
        assertResult(SpillFramework.stores.hostStore.spill(hostSize))(hostSize)
        withResource(handle.materialize(dataTypes)) { actualBatch =>
          TestUtils.compareBatches(expectedBatch, actualBatch)
        }
      }
    }
  }

  test("host originated: get host memory buffer") {
    val spillPriority = -10
    val hmb = HostMemoryBuffer.allocate(1L * 1024)
    val spillableBuffer = SpillableHostBuffer(hmb, hmb.getLength, spillPriority)
    withResource(spillableBuffer) { _ =>
      // the refcount of 1 is the store
      assertResult(1)(hmb.getRefCount)
      withResource(spillableBuffer.getHostBuffer()) { memoryBuffer =>
        assertResult(hmb)(memoryBuffer)
        assertResult(2)(memoryBuffer.getRefCount)
      }
    }
    assertResult(0)(hmb.getRefCount)
  }

  test("host originated: get host memory buffer after spill to disk") {
    val spillPriority = -10
    val hmb = HostMemoryBuffer.allocate(1L * 1024)
    val spillableBuffer = SpillableHostBuffer(
      hmb,
      hmb.getLength,
      spillPriority)
    assertResult(1)(hmb.getRefCount)
    //  we spill it
    SpillFramework.stores.hostStore.spill(hmb.getLength)
    withResource(spillableBuffer) { _ =>
      // the refcount of the original buffer is 0 because it spilled
      assertResult(0)(hmb.getRefCount)
      withResource(spillableBuffer.getHostBuffer()) { memoryBuffer =>
        assertResult(memoryBuffer.getLength)(hmb.getLength)
      }
    }
  }

  test("host originated: a buffer is not spillable when we leak it") {
    val spillPriority = -10
    val hmb = HostMemoryBuffer.allocate(1L * 1024)
    withResource(SpillableHostBuffer(hmb, hmb.getLength, spillPriority)) { spillableBuffer =>
      withResource(spillableBuffer.getHostBuffer()) { _ =>
        assertResult(0)(SpillFramework.stores.hostStore.spill(hmb.getLength))
      }
      assertResult(hmb.getLength)(SpillFramework.stores.hostStore.spill(hmb.getLength))
    }
  }

  test("host originated: a host batch is not spillable when we leak it") {
    val (hostCb, sparkTypes) = buildHostBatch()
    val sizeOnHost = RapidsHostColumnVector.getTotalHostMemoryUsed(hostCb)
    withResource(SpillableHostColumnarBatchHandle(hostCb)) { handle =>
      assertResult(true)(handle.spillable)

      withResource(handle.materialize(sparkTypes)) { _ =>
        // 0 because we have a reference to the host batch
        assertResult(false)(handle.spillable)
        assertResult(0)(SpillFramework.stores.hostStore.spill(sizeOnHost))
      }

      // after closing we still have 0 bytes in the store or available to spill
      assertResult(true)(handle.spillable)
    }
  }

  test("host originated: a host batch is not spillable when columns are incRefCounted") {
    val (hostCb, sparkTypes) = buildHostBatch()
    val sizeOnHost = RapidsHostColumnVector.getTotalHostMemoryUsed(hostCb)
    withResource(SpillableHostColumnarBatchHandle(hostCb)) { handle =>
      assertResult(true)(handle.spillable)
      val leakedFirstColumn = withResource(handle.materialize(sparkTypes)) { cb =>
        // 0 because we have a reference to the host batch
        assertResult(false)(handle.spillable)
        assertResult(0)(SpillFramework.stores.hostStore.spill(sizeOnHost))
        // leak it by increasing the ref count of the underlying cuDF column
        RapidsHostColumnVector.extractBases(cb).head.incRefCount()
      }
      withResource(leakedFirstColumn) { _ =>
        // 0 because we have a reference to the first column
        assertResult(false)(handle.spillable)
        assertResult(0)(SpillFramework.stores.hostStore.spill(sizeOnHost))
      }
      // batch is now spillable because we close our reference to the column
      assertResult(true)(handle.spillable)
      assertResult(sizeOnHost)(SpillFramework.stores.hostStore.spill(sizeOnHost))
    }
  }

  test("host originated: an aliased host batch is not spillable (until closing the original) ") {
    val (hostBatch, sparkTypes) = buildHostBatch()
    val handle = SpillableHostColumnarBatchHandle(hostBatch)
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(true)(handle.spillable)
      withResource(handle.materialize(sparkTypes)) { _ =>
        assertResult(false)(handle.spillable)
      } // we now have two copies in the store
      assertResult(true)(handle.spillable)
    }
  }

  test("host originated: a host batch supports duplicated columns") {
    val (hostBatch, sparkTypes) = buildHostBatchWithDuplicate()
    val handle = SpillableHostColumnarBatchHandle(hostBatch)
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(true)(handle.spillable)
      withResource(handle.materialize(sparkTypes)) { _ =>
        assertResult(false)(handle.spillable)
      } // we now have two copies in the store
      assertResult(true)(handle.spillable)
    }
  }

  test("host originated: a host batch supports aliasing and duplicated columns") {
    SpillFramework.shutdown()
    val sc = new SparkConf
    // disables the host store limit by enabling off heap limits
    sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "true")
    SpillFramework.initialize(new RapidsConf(sc))

    try {
      val (hostBatch, sparkTypes) = buildHostBatchWithDuplicate()
      withResource(SpillableHostColumnarBatchHandle(hostBatch)) { handle =>
        withResource(SpillableHostColumnarBatchHandle(handle.materialize(sparkTypes))) { handle2 =>
          assertResult(2)(SpillFramework.stores.hostStore.numHandles)
          assertResult(false)(handle.spillable)
          assertResult(false)(handle2.spillable)
        }
        assertResult(true)(handle.spillable)
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  // this is a key behavior that we wanted to keep during the spill refactor
  // where host objects that are added directly to the store do not cause a
  // host->disk spill on their own, instead they will get spilled later
  // due to device->host spills.
  test("host factory methods do not spill on addition") {
    SpillFramework.shutdown()
    val sc = new SparkConf
    // set a very small store size
    sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1KB")
    SpillFramework.initialize(new RapidsConf(sc))

    try {
      // add a lot of batches, surpassing the limits of the store
      val handles = new ArrayBuffer[SpillableHostColumnarBatchHandle]()
      var dataTypes: Array[DataType] = null
      (0 until 100).foreach { _ =>
        val (hostBatch, dt) = buildHostBatch()
        if (dataTypes == null) {
          dataTypes = dt
        }
        handles.append(SpillableHostColumnarBatchHandle(hostBatch))
      }
      // no spill to disk
      assertResult(100)(SpillFramework.stores.hostStore.numHandles)

      val dmb = DeviceMemoryBuffer.allocate(1024)
      withResource(SpillableDeviceBufferHandle(dmb)) { _ =>
        // simulate an OOM by spilling device memory
        SpillFramework.stores.deviceStore.spill(1024)
        assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      }

      val buffersSpilledToDisk = SpillFramework.stores.diskStore.numHandles
      // we spilled to disk
      assert(SpillFramework.stores.diskStore.numHandles > 0)
      // and the remaining objects that didn't spill, are still in the host store
      assertResult(100 - buffersSpilledToDisk)(SpillFramework.stores.hostStore.numHandles)
      assert(SpillFramework.stores.hostStore.totalSize <= 1024)
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("direct spill to disk: when buffer exceeds maximum size") {
    var (bigTable, sparkTypes) = buildTableOfLongs(2 * 1024 * 1024)
    closeOnExcept(bigTable) { _ =>
      // make a copy of the table so we can compare it later to the
      // one reconstituted after the spill
      val expectedBatch =
        withResource(bigTable.getTable.contiguousSplit()) { expectedTable =>
          GpuColumnVector.from(expectedTable(0).getTable, sparkTypes)
        }
      withResource(expectedBatch) { _ =>
        withResource(SpillableColumnarBatchFromBufferHandle(
          bigTable, sparkTypes)) { bigHandle =>
          bigTable = null
          withResource(bigHandle.materialize(sparkTypes)) { actualBatch =>
            TestUtils.compareBatches(expectedBatch, actualBatch)
          }
          SpillFramework.stores.deviceStore.spill(bigHandle.approxSizeInBytes)
          assertResult(true)(bigHandle.dev.isEmpty)
          assertResult(true)(bigHandle.host.get.host.isEmpty)
          assertResult(false)(bigHandle.host.get.disk.isEmpty)

          withResource(bigHandle.materialize(sparkTypes)) { actualBatch =>
            TestUtils.compareBatches(expectedBatch, actualBatch)
          }
        }
      }
    }
  }

  test("get columnar batch after spilling to disk") {
    val (size, handle, dataTypes) = addContiguousTableToFramework()
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore
    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)

      val expectedTable =
        withResource(handle.materialize(dataTypes)) { beforeSpill =>
          withResource(GpuColumnVector.from(beforeSpill)) { table =>
            table.contiguousSplit()(0)
          }
        } // closing the batch from the store so that we can spill it

      withResource(expectedTable) { _ =>
        withResource(
          GpuColumnVector.from(expectedTable.getTable, dataTypes)) { expectedBatch =>
          deviceStore.spill(size)
          hostStore.spill(size)

          assertResult(0)(deviceStore.numHandles)
          assertResult(0)(hostStore.numHandles)
          assertResult(1)(diskStore.numHandles)

          val diskHandle = handle.host.flatMap(_.disk).get
          val path = diskStore.getFile(diskHandle.blockId)
          assert(path.exists)
          withResource(handle.materialize(dataTypes)) { actualBatch =>
            TestUtils.compareBatches(expectedBatch, actualBatch)
          }
        }
      }
    }
  }

  // -1 disables the host store limit
  val hostSpillStorageSizes = Seq("-1", "1MB", "16MB")
  val spillToDiskBounceBuffers = Seq("128KB", "2MB", "128MB")
  val chunkedPackBounceBuffers = Seq("1MB", "8MB", "128MB")
  hostSpillStorageSizes.foreach { hostSpillStorageSize =>
    spillToDiskBounceBuffers.foreach { spillToDiskBounceBufferSize =>
      chunkedPackBounceBuffers.foreach { chunkedPackBounceBufferSize =>
        test("materialize non-contiguous batch after " +
          s"host_storage_size=$hostSpillStorageSize " +
          s"spilling chunked_pack_bb=$chunkedPackBounceBufferSize " +
          s"spill_to_disk_bb=$spillToDiskBounceBufferSize") {
          SpillFramework.shutdown()
          try {
            val sc = new SparkConf
            sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, hostSpillStorageSize)
            sc.set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key, chunkedPackBounceBufferSize)
            sc.set(RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_SIZE.key, spillToDiskBounceBufferSize)
            SpillFramework.initialize(new RapidsConf(sc))
            val (largeTable, dataTypes) = buildNonContiguousTableOfLongs(numRows = 1000000)
            val handle = SpillableColumnarBatchHandle(largeTable, dataTypes)
            val diskStore = SpillFramework.stores.diskStore
            val hostStore = SpillFramework.stores.hostStore
            val deviceStore = SpillFramework.stores.deviceStore
            withResource(handle) { _ =>
              assertResult(1)(deviceStore.numHandles)
              assertResult(0)(diskStore.numHandles)
              assertResult(0)(hostStore.numHandles)

              val expectedTable =
                withResource(handle.materialize(dataTypes)) { beforeSpill =>
                  withResource(GpuColumnVector.from(beforeSpill)) { table =>
                    table.contiguousSplit()(0)
                  }
                } // closing the batch from the store so that we can spill it

              withResource(expectedTable) { _ =>
                withResource(
                  GpuColumnVector.from(expectedTable.getTable, dataTypes)) { expectedBatch =>
                  deviceStore.spill(handle.approxSizeInBytes)
                  hostStore.spill(handle.approxSizeInBytes)

                  assertResult(0)(deviceStore.numHandles)
                  assertResult(0)(hostStore.numHandles)
                  assertResult(1)(diskStore.numHandles)

                  val diskHandle = handle.host.flatMap(_.disk).get
                  val path = diskStore.getFile(diskHandle.blockId)
                  assert(path.exists)
                  withResource(handle.materialize(dataTypes)) { actualBatch =>
                    TestUtils.compareBatches(expectedBatch, actualBatch)
                  }
                }
              }
            }
          } finally {
            SpillFramework.shutdown()
          }
        }
      }
    }
  }

  test("get memory buffer after spilling to disk") {
    val handle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(123))
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore
    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      val expectedBuffer =
        withResource(handle.materialize()) { devbuf =>
          closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
            hostbuf.copyFromDeviceBuffer(devbuf)
            hostbuf
          }
        }
      withResource(expectedBuffer) { expectedBuffer =>
        deviceStore.spill(handle.approxSizeInBytes)
        hostStore.spill(handle.approxSizeInBytes)
        withResource(handle.host.map(_.materialize()).get) { actualHostBuffer =>
          assertResult(expectedBuffer.
            asByteBuffer.limit())(actualHostBuffer.asByteBuffer.limit())
        }
      }
    }
  }

  test("Compression on with or without encryption for spill block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, false)
    }
  }

  test("Compression off with or without encryption for spill block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, false)
    }
  }

  test("Compression on with or without encryption for spill block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, false)
    }
  }

  test("Compression off with or without encryption for spill block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, false)
    }
  }

  // ===== Tests for shuffle block =====

  test("Compression on with or without encryption for shuffle block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, true)
    }
  }

  test("Compression off with or without encryption for shuffle block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, true)
    }
  }

  test("Compression on with or without encryption for shuffle block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, true, true)
    }
  }

  test("Compression off with or without encryption for shuffle block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, true, true)
    }
  }

  test("No encryption and compression for shuffle block using multiple batches") {
    readWriteTestWithBatches(new SparkConf(), true, true)
  }

  private def readWriteTestWithBatches(conf: SparkConf, shareDiskPaths: Boolean*) = {
    assert(shareDiskPaths.nonEmpty)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(conf))

    shareDiskPaths.foreach { _ =>
      val (_, handle, dataTypes) = addContiguousTableToFramework()
      withResource(handle) { _ =>
        val expectedCt = withResource(handle.materialize(dataTypes)) { devbatch =>
          withResource(GpuColumnVector.from(devbatch)) { tmpTbl =>
            tmpTbl.contiguousSplit()(0)
          }
        }
        withResource(expectedCt) { _ =>
          val expectedBatch = withResource(expectedCt.getTable) { expectedTbl =>
            GpuColumnVector.from(expectedTbl, dataTypes)
          }
          withResource(expectedBatch) { _ =>
            assertResult(true)(
              SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes) > 0)
            assertResult(true)(
              SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes) > 0)
            withResource(handle.materialize(dataTypes)) { actualBatch =>
              TestUtils.compareBatches(expectedBatch, actualBatch)
            }
          }
        }
      }
    }
  }

  test("skip host: spill device memory buffer to disk") {
    SpillFramework.shutdown()
    try {
      val sc = new SparkConf
      // disables the host store limit
      sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1KB")
      SpillFramework.initialize(new RapidsConf(sc))
      // buffer is too big for host store limit, so we will skip host
      val handle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(1025))
      val deviceStore = SpillFramework.stores.deviceStore
      withResource(handle) { _ =>
        val expectedBuffer =
          withResource(handle.materialize()) { devbuf =>
            closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
              hostbuf.copyFromDeviceBuffer(devbuf)
              hostbuf
            }
          }

        withResource(expectedBuffer) { _ =>
          // host store will fail to spill
          deviceStore.spill(handle.approxSizeInBytes)
          assert(handle.host.map(_.host.isEmpty).get)
          assert(handle.host.map(_.disk.isDefined).get)
          withResource(handle.host.map(_.materialize()).get) { buffer =>
            assertResult(expectedBuffer.asByteBuffer)(buffer.asByteBuffer)
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("skip host: spill table to disk") {
    SpillFramework.shutdown()
    try {
      val sc = new SparkConf
      sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1KB")
      SpillFramework.initialize(new RapidsConf(sc))
      // fill up the host store
      withResource(SpillableHostBufferHandle(HostMemoryBuffer.allocate(1024))) { hostHandle =>
        // make sure the host handle isn't spillable
        withResource(hostHandle.materialize()) { _ =>
          val (handle, _) = addTableToFramework()
          withResource(handle) { _ =>
            val (expectedTable, dataTypes) = buildTable()
            withResource(expectedTable) { _ =>
              withResource(
                GpuColumnVector.from(expectedTable, dataTypes)) { expectedBatch =>
                SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
                assert(handle.host.map(_.host.isEmpty).get)
                assert(handle.host.map(_.disk.isDefined).get)
                withResource(handle.materialize(dataTypes)) { fromDiskBatch =>
                  TestUtils.compareBatches(expectedBatch, fromDiskBatch)
                  assert(handle.dev.isEmpty)
                  assert(handle.host.map(_.host.isEmpty).get)
                  assert(handle.host.map(_.disk.isDefined).get)
                }
              }
            }
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("skip host: spill table to disk with small host bounce buffer") {
    try {
      SpillFramework.shutdown()
      val sc = new SparkConf
      // make this super small so we skip the host
      sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1")
      sc.set(RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_SIZE.key, "10")
      sc.set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key, "1MB")
      val rapidsConf = new RapidsConf(sc)
      SpillFramework.initialize(rapidsConf)
      val (handle, _) = addTableToFramework()
      withResource(handle) { _ =>
        val (expectedTable, dataTypes) = buildTable()
        withResource(expectedTable) { _ =>
          withResource(
            GpuColumnVector.from(expectedTable, dataTypes)) { expectedBatch =>
            SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
            assert(handle.dev.isEmpty)
            assert(handle.host.map(_.host.isEmpty).get)
            assert(handle.host.map(_.disk.isDefined).get)
            withResource(handle.materialize(dataTypes)) { fromDiskBatch =>
              TestUtils.compareBatches(expectedBatch, fromDiskBatch)
            }
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("0-byte table is never spillable") {
    val (handle, _) = addZeroRowsTableToFramework()
    val (handle2, _) = addTableToFramework()

    withResource(handle) { _ =>
      withResource(handle2) { _ =>
        assert(handle2.host.isEmpty)
        val (expectedTable, expectedTypes) = buildTable()
        withResource(expectedTable) { _ =>
          withResource(
            GpuColumnVector.from(expectedTable, expectedTypes)) { expectedCb =>
            SpillFramework.stores.deviceStore.spill(
              handle.approxSizeInBytes + handle2.approxSizeInBytes)
            SpillFramework.stores.hostStore.spill(
              handle.approxSizeInBytes + handle2.approxSizeInBytes)
            // the 0-byte table never moved from device. It is not spillable
            assert(handle.host.isEmpty)
            assert(!handle.spillable)
            // the second table (with rows) did spill
            assert(handle2.host.isDefined)
            assert(handle2.host.map(_.host.isEmpty).get)
            assert(handle2.host.map(_.disk.isDefined).get)

            withResource(handle2.materialize(expectedTypes)) { spilledBatch =>
              TestUtils.compareBatches(expectedCb, spilledBatch)
            }
          }
        }
      }
    }
  }

  test("exclusive spill files are deleted when buffer deleted") {
    testBufferFileDeletion(canShareDiskPaths = false)
  }

  test("shared spill files are not deleted when a buffer is deleted") {
    testBufferFileDeletion(canShareDiskPaths = true)
  }

  def testCloseWhileSpilling[T <: SpillableHandle](handle: T, store: SpillableStore[T],
                                                   sleepBeforeCloseNanos: Long): Unit = {
    assert(handle.spillable)
    assertResult(1)(store.numHandles)
    val t1 = new Thread (() => {
      // cannot assert how much is spills because it depends on whether the handle
      // is already closed or not and we're trying to force both conditions
      // in this test to show that it handles potential races correctly
      store.spill(handle.approxSizeInBytes)
    })
    t1.start()

    // we observed that the race will typically trigger if sleeping between 0.1 and 1 millis
    Thread.sleep(sleepBeforeCloseNanos / 1000000L, (sleepBeforeCloseNanos % 1000000L).toInt)
    handle.close()
    t1.join()
    assertResult(0)(store.numHandles)
  }

  // This is a small monte carlo simulation where we test overlaying
  // closing buffers and spilling at difference delay points to tease out possible
  // race conditions. There's only one param/variable in the simulation, but it could
  // be extended to N params if needed
  def monteCarlo(oneIteration: Long => Unit): Unit = {
    for (i <- 1L to 10L) {
      val nanos: Long = i * 100 * 1000
      oneIteration(nanos)
    }
  }

  test("a non-contiguous table close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (tbl, dataTypes) = buildTable()
      val handle = SpillableColumnarBatchHandle(tbl, dataTypes)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("a device buffer close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (ct, _) = buildContiguousTable()
      // the contract for spillable handles is that they take ownership
      // incRefCount to follow that pattern
      val buff = ct.getBuffer
      buff.incRefCount()
      val handle = SpillableDeviceBufferHandle(buff)
      ct.close()
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("host columnar batch close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (hostCb, _) = buildHostBatch()
      val handle = SpillableHostColumnarBatchHandle(hostCb)
      testCloseWhileSpilling(handle, SpillFramework.stores.hostStore, sleepBeforeCloseNanos)
    }
  }

  test("host memory buffer close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val handle = SpillableHostBufferHandle(HostMemoryBuffer.allocate(1024))
      testCloseWhileSpilling(handle, SpillFramework.stores.hostStore, sleepBeforeCloseNanos)
    }
  }

  test("cb from buffer handle close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (ct, dataTypes) = buildContiguousTable()
      val handle = SpillableColumnarBatchFromBufferHandle(ct, dataTypes)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("compressed cb handle close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val ct = buildCompressedBatch(0, 1000)
      val handle = SpillableCompressedColumnarBatchHandle(ct)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

}
