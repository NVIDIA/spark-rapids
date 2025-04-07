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

package com.nvidia.spark.rapids

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream,
  File,
  FileOutputStream
}
import java.nio.file.Files

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, Table}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import org.scalatest.funsuite.AnyFunSuite

class DumpUtilsSuite extends AnyFunSuite {

  test("dumpToParquet handles SerializedTableColumn") {
    // Create a simple table
    withResource(
      new Table.TestBuilder()
        .column(
          1L.asInstanceOf[java.lang.Long],
          2L.asInstanceOf[java.lang.Long],
          3L.asInstanceOf[java.lang.Long])
        .column("a", "b", "c")
        .build()) { table =>
      // Serialize the table to a SerializedTableColumn
      val byteOut = new ByteArrayOutputStream()
      val out = new DataOutputStream(byteOut)
      JCudfSerialization.writeToStream(table, out, 0, table.getRowCount)
      out.close()
      val bytes = byteOut.toByteArray
      val in = new ByteArrayInputStream(bytes)
      val din = new DataInputStream(in)
      val header = new SerializedTableHeader(din)
      assert(header.wasInitialized())
      closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen)) { hostBuffer =>
        JCudfSerialization.readTableIntoBuffer(din, header, hostBuffer)
        assert(header.wasDataRead())
        // Create a ColumnarBatch with a SerializedTableColumn
        withResource(SerializedTableColumn.from(header, hostBuffer)) { batch =>
          // Create a temporary output file
          val tempFile = File.createTempFile("dumptest", ".parquet")
          tempFile.deleteOnExit()
          // Test dumping the SerializedTableColumn to parquet
          withResource(new FileOutputStream(tempFile)) { fileOut =>
            DumpUtils.dumpToParquet(batch, fileOut)
          }
          // Verify the file was created and has content
          assert(tempFile.exists())
          assert(tempFile.length() > 0)
          // Clean up
          Files.delete(tempFile.toPath)
        }
      }
    }
  }
}
