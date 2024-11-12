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

package org.apache.spark.rapids.velox

import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{GpuParquetUtils, HostMemoryOutputStream}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.velox.VeloxBackendApis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC

import org.apache.spark.internal.Logging

case class FileCopyRange(offset: Long, length: Long, targetMemAddr: Long)

object VeloxHDFS extends Logging {

  private val PARQUET_FOOTER_LENGTH_SIZE = 4

  /**
   * Returns object handle of VeloxHadoopInputFileStream. Returns 0 if fails.
   */
  def createInputFileStream(filePath: String): Long = {
    if (!isVeloxHDFSEnabled) {
      return 0L
    }
    // If openHadoopFile returns 0, it means the specific filePath cannot be parsed by veloxHDFS
    val ptr = runtime.hdfsOpenFile(filePath)
    if (ptr == 0L) {
      logError(s"failed to open file($filePath) via VeloxHDFS")
    }
    ptr
  }

  /**
   * Reads range data contiguously from HDFS files via VeloxHDFS.
   */
  def copyRangesFromFile(fileName: String,
                         fileStream: Long,
                         ranges: Seq[FileCopyRange],
                         closeAfterFinished: Boolean = false): Unit = {
    try {
      ranges.foreach {
        case FileCopyRange(offset, len, tgtAddr) =>
          runtime.hdfsCopyFromFile(fileStream, offset, len, tgtAddr, false)
      }
      logError(s"successfully read file($fileName) through VeloxHDFS")
    } finally {
      if (closeAfterFinished) {
        runtime.hdfsCloseFile(fileStream)
      }
    }
  }

  /**
   * The alternative implementation of GpuParquetFileFilterHandler.readFooterBufUsingHadoop
   * via VeloxHDFS (backed by libhdfs3) instead of parquet-mr
   */
  def readParquetFooterBuffer(filePath: Path, conf: Configuration): Option[HostMemoryBuffer] = {
    if (!isVeloxHDFSEnabled) {
      return None
    }
    // If openHadoopFile returns 0, it means the specific filePath cannot be parsed by veloxHDFS
    val handle = runtime.hdfsOpenFile(filePath.toString)
    if (handle == 0L) {
      logError(s"failed to open file($filePath) via VeloxHDFS")
      return None
    }

    val fileLen = runtime.hdfsGetFileSize(handle)
    if (fileLen < MAGIC.length + PARQUET_FOOTER_LENGTH_SIZE + MAGIC.length) {
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen )")
    }
    val footerLengthIndex = fileLen - PARQUET_FOOTER_LENGTH_SIZE - MAGIC.length
    try {
      withResource(new NvtxRange("ReadFooterBytes", NvtxColor.YELLOW)) { _ =>
        runtime.hdfsReaderSeekTo(handle, footerLengthIndex)
        val footerLength = runtime.hdfsReadIntLittleEndian(handle)
        val magic = new Array[Byte](MAGIC.length)
        withResource(HostMemoryBuffer.allocate(MAGIC.length, false)) { buf =>
          runtime.hdfsReadFully(handle, buf.getAddress, buf.getLength)
          buf.getBytes(magic, 0, 0, buf.getLength)
        }
        val footerIndex = footerLengthIndex - footerLength
        GpuParquetUtils.verifyParquetMagic(filePath, magic)
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
          throw new RuntimeException(s"corrupted file: the footer index is not within " +
            s"the file: $footerIndex")
        }
        val hmbLength = (fileLen - footerIndex).toInt
        closeOnExcept(HostMemoryBuffer.allocate(hmbLength + MAGIC.length, false)) { buf =>
          val out = new HostMemoryOutputStream(buf)
          out.write(MAGIC)
          // close HadoopFileInputStream after read
          runtime.hdfsCopyFromFile(handle, footerIndex, hmbLength,
            buf.getAddress + out.getPos,
            true
          )
          logError(s"successfully read ParquetFooter($filePath) through VeloxHDFS")
          Some(buf)
        }
      }
    } catch {
      case ex: Throwable =>
        runtime.hdfsCloseFile(handle)
        throw ex
    }
  }

  // It's safe to assert VeloxRuntime is defined when useVeloxHdfs returns true, because
  // useVeloxHdfs shall certainly return false if VeloxRuntime has NOT been setup.
  @transient
  private lazy val isVeloxHDFSEnabled = VeloxBackendApis.useVeloxHdfs

  @transient
  private lazy val runtime = VeloxBackendApis.getRuntime.get

}
