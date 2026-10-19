/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.parquet._
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.{MessageType, MessageTypeParser, Type}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{IntegerType, StructType}

class ParquetCombineSuite extends RmmSparkRetrySuiteBase {
  private val dataSchema = MessageTypeParser.parseMessageType("message test { optional int32 b; }")
  private val emptySchema = new MessageType("test", java.util.Collections.emptyList[Type]())
  private val readSchema = new StructType().add("b", IntegerType)

  override def beforeEach(): Unit = {
    super.beforeEach()
    TrampolineUtil.setTaskContext(new MockTaskContext(1, 0))
  }

  override def afterEach(): Unit = {
    try {
      TrampolineUtil.unsetTaskContext()
    } finally {
      super.afterEach()
    }
  }

  private class Reader(ordered: Boolean) extends MultiFileCloudParquetPartitionReader(
      null, new Configuration(false), Array.empty, _ => throw new UnsupportedOperationException,
      true, None, false, 1000, 1L << 20, 1L << 20, 1L << 20, false, 0,
      false, CpuCompressionConfig.disabled(), Map.empty, new StructType(),
      new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false).build(),
      1, false, false, false, false, ordered, CombineConf(1L << 20, 0)) {
    var failRowCount = false
    var failBuild = false

    private def file(key: Int) = PartitionedFileUtilsShim.newPartitionedFile(
      InternalRow(key), s"file:///tmp/part-$key", 0, 0)

    def empty(key: Int, rows: Long): HostMemoryEmptyMetaData =
      newHMEmptyMetadataForChunks(file(key), 0, 1, DateTimeRebaseCorrected,
        DateTimeRebaseCorrected, false, if (rows == 0) null else emptySchema,
        if (rows == 0) null else readSchema, rows, Seq.empty)

    def data(key: Int, rows: Long, schema: MessageType = dataSchema)
        : HostMemoryBuffersWithMetaData = {
      val partFile = file(key)
      val meta = ParquetFileInfoWithBlockMeta(new Path(partFile.filePath.toString()),
        Seq.empty, partFile.partitionValues, schema, readSchema,
        DateTimeRebaseCorrected, DateTimeRebaseCorrected, false)
      newHMBWithMetaDataForChunks(partFile, Array(SingleHMBAndMeta.empty(rows)), 1, meta)
    }

    def pending: Array[HostMemoryBuffersWithMetaDataBase] =
      combineLeftOverFiles.getOrElse(Array.empty)

    def combinePending(): HostMemoryBuffersWithMetaDataBase = {
      val input = pending
      combineLeftOverFiles = None
      combineHMBs(input)
    }

    override protected def computeNumRowsAlive(rows: Long,
        metadata: HostMemoryBuffersWithMetaDataBase): Int = {
      require(!failRowCount, "injected row-count failure")
      super.computeNumRowsAlive(rows, metadata)
    }

    override protected def newCombinedHMBWithMetaData(meta: CombinedMeta,
        buffer: SingleHMBAndMeta, offset: Long): HostMemoryBuffersWithMetaData = {
      require(!failBuild, "injected combined-buffer failure")
      super.newCombinedHMBWithMetaData(meta, buffer, offset)
    }
  }

  private def keys(meta: HostMemoryBuffersWithMetaDataBase): Seq[Int] =
    meta.allPartValues.map(_.map(_._2.getInt(0)).toSeq)
      .getOrElse(Seq(meta.partitionedFile.partitionValues.getInt(0)))

  for (ordered <- Seq(true, false)) {
    test(s"consecutive metadata-only files combine and preserve partitions, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        var released = 0
        val n1 = reader.empty(0, 10)
        val n2 = reader.empty(1, 20)
        n1.addReleaseResourceCallback(() => released += 1)
        n2.addReleaseResourceCallback(() => released += 1)
        val d = reader.data(2, 30)
        withResource(reader.combineHMBs(Array(n1, n2, d))) { combined =>
          assert(combined.isInstanceOf[HostMemoryEmptyMetaData])
          assert(combined.memBuffersAndSizes.map(_.numRows).sum == 30)
          assert(keys(combined) == Seq(0, 1))
          assert(combined.allPartValues.get.map(_._1).toSeq == Seq(10L, 20L))
          assert(released == 0)
          assert(reader.pending.toSeq == Seq(d))
        }
        assert(released == 2)
        withResource(reader.combinePending()) { combined => assert(combined eq d) }
      }
    }

    test(s"buffered groups defer metadata-only rows, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        val d1 = reader.data(0, 10)
        val n = reader.empty(1, 20)
        val d2 = reader.data(2, 30)
        withResource(reader.combineHMBs(Array(d1, n, d2))) { combined =>
          assert(combined.isInstanceOf[HostMemoryBuffersWithMetaData])
          assert(keys(combined) == (if (ordered) Seq(0) else Seq(0, 2)))
          assert(reader.pending.toSeq == (if (ordered) Seq(n, d2) else Seq(n)))
        }
        withResource(reader.combinePending()) { combined =>
          assert(combined eq n)
        }
        if (ordered) {
          withResource(reader.combinePending()) { combined => assert(combined eq d2) }
        }
      }
    }

    test(s"zero-row sentinels do not split metadata-only groups, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        withResource(reader.combineHMBs(Array(reader.empty(0, 0), reader.empty(1, 10),
            reader.empty(2, 0), reader.empty(3, 20)))) { combined =>
          val meta = combined.asInstanceOf[HostMemoryEmptyMetaData]
          assert(meta.numRows == 30)
          assert(meta.readSchema == readSchema)
          assert(keys(meta) == Seq(0, 1, 2, 3))
          assert(reader.pending.isEmpty)
        }
      }
    }

    test(s"existing schema splits remain compatible with metadata-only groups, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        val otherSchema = MessageTypeParser.parseMessageType("message test { optional int64 b; }")
        val input = Array[HostMemoryBuffersWithMetaDataBase](reader.data(0, 10),
          reader.data(1, 20, otherSchema), reader.empty(2, 30), reader.data(3, 40))
        withResource(reader.combineHMBs(input)) { combined =>
          assert(keys(combined) == (if (ordered) Seq(0) else Seq(0, 3)))
        }
        withResource(reader.combinePending()) { combined => assert(keys(combined) == Seq(1)) }
        withResource(reader.combinePending()) { combined => assert(keys(combined) == Seq(2)) }
        if (ordered) {
          withResource(reader.combinePending()) { combined => assert(keys(combined) == Seq(3)) }
        }
        assert(reader.pending.isEmpty)
      }
    }
  }

  test("a selected singleton retains its original chunk metadata") {
    withResource(new Reader(ordered = true)) { reader =>
      val data = reader.data(0, Int.MaxValue.toLong + 1)
      withResource(reader.combineHMBs(Array(data, reader.empty(1, 10)))) { combined =>
        assert(combined eq data)
      }
    }
  }

  test("row-count failure releases selected and deferred inputs") {
    withResource(new Reader(ordered = true)) { reader =>
      var released = 0
      val input = Array[HostMemoryBuffersWithMetaDataBase](
        reader.empty(0, 10), reader.empty(1, 20), reader.data(2, 30))
      input.foreach(_.addReleaseResourceCallback(() => released += 1))
      reader.failRowCount = true
      intercept[IllegalArgumentException] { reader.combineHMBs(input) }
      assert(released == 3)
      assert(reader.pending.isEmpty)
    }
  }

  test("combined-buffer failure releases selected and deferred inputs") {
    withResource(new Reader(ordered = true)) { reader =>
      var released = 0
      val input = Array[HostMemoryBuffersWithMetaDataBase](
        reader.data(0, 10), reader.data(1, 20), reader.empty(2, 30))
      input.foreach(_.addReleaseResourceCallback(() => released += 1))
      reader.failBuild = true
      intercept[IllegalArgumentException] { reader.combineHMBs(input) }
      assert(released == 3)
      assert(reader.pending.isEmpty)
    }
  }
}
