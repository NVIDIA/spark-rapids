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

package com.nvidia.spark.rapids.kudo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}
import java.util.concurrent.ThreadLocalRandom

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.TestUtils
import com.nvidia.spark.rapids.kudo.KudoSerializerUtils._
import org.scalatest.funsuite.AnyFunSuite

class KudoSerializerSuite extends AnyFunSuite {

  test("Serialize and Deserialize Table") {
    try {
      val expected = buildTestTable()
      val rowCount = expected.getRowCount.toInt
      for (sliceSize <- 1 to rowCount) {
        val tableSlices = new mutable.ListBuffer[TableSlice]()
        for (startRow <- 0 until rowCount by sliceSize) {
          tableSlices += TableSlice(startRow, Math.min(sliceSize, rowCount - startRow), expected)
        }
        checkMergeTable(expected, tableSlices)
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  test("Row Count Only") {
    val out = new OpenByteArrayOutputStream()
    val bytesWritten = KudoSerializer.writeRowCountToStream(out, 5)
    assert(bytesWritten == 28)

    val in = new ByteArrayInputStream(out.toByteArray)
    val header = KudoTableHeader.readFrom(new DataInputStream(in)).get()

    assert(header.getNumColumns == 0)
    assert(header.getOffset == 0)
    assert(header.getNumRows == 5)
    assert(header.getValidityBufferLen == 0)
    assert(header.getOffsetBufferLen == 0)
    assert(header.getTotalDataLen == 0)
  }

  test("Write Simple") {
    val serializer = new KudoSerializer(buildSimpleTestSchema())

    withResource(buildSimpleTable())(t => {
      val out = new OpenByteArrayOutputStream()
      val bytesWritten = serializer.writeToStreamWithMetrics(t, out, 0, 4).getWrittenBytes
      assert(bytesWritten == 189)

      val in = new ByteArrayInputStream(out.toByteArray)
      val header = KudoTableHeader.readFrom(new DataInputStream(in)).get()
      assert(header.getNumColumns == 7)
      assert(header.getOffset == 0)
      assert(header.getNumRows == 4)
      assert(header.getValidityBufferLen == 24)
      assert(header.getOffsetBufferLen == 40)
      assert(header.getTotalDataLen == 160)

      // First integer column has no validity buffer
      assert(!header.hasValidityBuffer(0))
      for (i <- 1 until 7) {
        assert(header.hasValidityBuffer(i))
      }
    })
  }

  test("Merge Table With Different Validity") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {
      val table1 = new Table.TestBuilder()
        .column(Long.box(-83182L), 5822L, 3389L, 7384L, 7297L)
        .column(-2.06, -2.14, 8.04, 1.16, -1.0)
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column(Long.box(-47L), null, -83L, -166L, -220L, 470L, 619L, 803L, 661L)
        .column(-6.08, 1.6, 1.78, -8.01, 1.22, 1.43, 2.13, -1.65, null)
        .build()
      tables.+=(table2)

      val table3 = new Table.TestBuilder()
        .column(Long.box(8722L), 8733L)
        .column(2.51, 0.0)
        .build()
      tables.+=(table3)

      val expected = new Table.TestBuilder()
        .column(Long.box(7384L), 7297L, 803L, 661L, 8733L)
        .column(1.16, -1.0, -1.65, null, 0.0)
        .build()
      tables.+=(expected)

      checkMergeTable(expected, Seq(
        new TableSlice(3, 2, table1),
        new TableSlice(7, 2, table2),
        new TableSlice(1, 1, table3)
      ))
      null
    }
    }
  }

  test("Merge String") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {
      val table1 = new Table.TestBuilder()
        .column("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4", "5", "6", "7",
          null, "9", "10", "11", "12", "13", null, "15")
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column("A", "A", "C", "C", "E", "TESTING", "1", "2", "3", "4", "5", "6", "7",
          "", "9", "10", "11", "12", "13", "", "15")
        .build()
      tables.+=(table2)

      val expected = new Table.TestBuilder()
        .column("C", "D", null, "TESTING", "1", "2", "3", "4", "5", "6", "7",
          null, "9", "C", "E", "TESTING", "1", "2")
        .build()
      tables.+=(expected)

      checkMergeTable(expected, Seq(
        new TableSlice(2, 13, table1),
        new TableSlice(3, 5, table2)
      ))
      null
    }
    }
  }

  test("Merge List") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {

      val table1 = new Table.TestBuilder()
        .column(Long.box(-881L), 482L, 660L, 896L, -129L, -108L, -428L, 0L, 617L, 782L)
        .column(integers(665), integers(-267), integers(398), integers(-314),
          integers(-370), integers(181), integers(665, 544), integers(222),
          integers(-587), integers(544))
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column(Long.box(-881L), 482L, 660L, 896L, 122L, 241L, 281L, 680L, 783L, null)
        .column(integers(-370), integers(398), integers(-587, 398), integers(-314), integers(307),
          integers(-397, -633), integers(-314, 307), integers(-633), integers(-397),
          integers(181, -919, -175))
        .build()
      tables.+=(table2)

      val expected = new Table.TestBuilder()
        .column(Long.box(896L), -129L, -108L, -428L, 0L, 617L, 782L, 482L,
          660L, 896L, 122L, 241L, 281L, 680L, 783L, null)
        .column(integers(-314), integers(-370), integers(181), integers(665, 544),
          integers(222), integers(-587), integers(544), integers(398), integers(-587, 398),
          integers(-314), integers(307), integers(-397, -633), integers(-314, 307),
          integers(-633), integers(-397), integers(181, -919, -175))
        .build()
      tables.+=(expected)

      checkMergeTable(expected, Seq(
        new TableSlice(3, 7, table1),
        new TableSlice(1, 9, table2)
      ))
      null
    }
    }
  }

  test("Merge Complex Struct List") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {

      val listMapType = new HostColumnVector.ListType(true,
        new HostColumnVector.ListType(true,
          new HostColumnVector.StructType(true,
            new HostColumnVector.BasicType(false, DType.STRING),
            new HostColumnVector.BasicType(true, DType.STRING))))

      val table = new Table.TestBuilder()
        .column(listMapType,
          List(
            List(struct("k1", "v1"), struct("k2", "v2")).asJava,
            List(struct("k3", "v3")).asJava).asJava,
          null,
          List(
            List(struct("k14", "v14"), struct("k15", "v15")).asJava
          ).asJava,
          null,
          List(null, null, null).asJava,
          List(
            List(struct("k22", null)).asJava,
            List(struct("k23", null)).asJava
          ).asJava, null, null, null)
        .build()
      tables.+=(table)

      checkMergeTable(table, Seq(
        new TableSlice(0, 3, table),
        new TableSlice(3, 3, table),
        new TableSlice(6, 3, table)
      ))
      null
    }
    }
  }

  test("Serialize Validity") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {

      val col1 = mutable.ListBuffer[Integer]()
      col1.+=(null)
      col1.+=(null)
      2 until 512 foreach { i => col1.+=(i) }

      val table1 = new Table.TestBuilder()
        .column(col1: _*)
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column(Int.box(509), 510, 511)
        .build()
      tables.+=(table2)

      checkMergeTable(table2, Seq(new TableSlice(509, 3, table1)))
      null
    }
    }
  }

  test("ByteArrayOutputStreamWriter") {
    val bout = new ByteArrayOutputStream(32)
    val writer = new ByteArrayOutputStreamWriter(bout)

    writer.writeInt(0x12345678)

    val testByteArr1 = new Array[Byte](2097)
    ThreadLocalRandom.current().nextBytes(testByteArr1)
    writer.write(testByteArr1, 0, testByteArr1.length)

    val testByteArr2 = new Array[Byte](7896)
    ThreadLocalRandom.current().nextBytes(testByteArr2)
    val buffer = HostMemoryBuffer.allocate(testByteArr2.length)
    try {
      buffer.setBytes(0, testByteArr2, 0, testByteArr2.length)
      writer.copyDataFrom(buffer, 0, testByteArr2.length)
    } finally {
      buffer.close()
    }

    val expected = new Array[Byte](4 + testByteArr1.length + testByteArr2.length)
    expected(0) = 0x12.toByte
    expected(1) = 0x34.toByte
    expected(2) = 0x56.toByte
    expected(3) = 0x78.toByte
    System.arraycopy(testByteArr1, 0, expected, 4, testByteArr1.length)
    System.arraycopy(testByteArr2, 0, expected, 4 + testByteArr1.length, testByteArr2.length)

    assert(expected.sameElements(bout.toByteArray))
  }

  case class TableSlice(startRow: Int, numRows: Int, baseTable: Table) {}

  private def checkMergeTable(expected: Table, tableSlices: Seq[TableSlice]): Unit = {
    try {
      val serializer = new KudoSerializer(schemaOf(expected))

      val bout = new OpenByteArrayOutputStream()
      for (slice <- tableSlices) {
        serializer.writeToStreamWithMetrics(slice.baseTable, bout, slice.startRow, slice.numRows)
      }
      bout.flush()

      val bin = new ByteArrayInputStream(bout.toByteArray)
      withResource(new mutable.ArrayBuffer[KudoTable](tableSlices.size)) { kudoTables => {
        try {
          for (i <- tableSlices.indices) {
            kudoTables.+=(KudoTable.from(bin).get)
          }

          val rows = kudoTables.map(t => t.getHeader.getNumRows).sum
          assert(expected.getRowCount === rows)

          val merged = serializer.mergeToTable(kudoTables.toArray)
          try {
            assert(expected.getRowCount === merged.getRowCount)
            TestUtils.compareTables(expected, merged)
          } finally {
            merged.close()
          }
        } catch {
          case e: Exception => throw new RuntimeException(e)
        }

        null
      }
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

}
