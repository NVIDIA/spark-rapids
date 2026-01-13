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

package com.nvidia.spark.rapids.sequencefile

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.{Text, VersionMismatchException}

/**
 * Parsed header information from a Hadoop SequenceFile.
 *
 * @param syncMarker The 16-byte sync marker used to identify record boundaries
 * @param headerSize Size of the header in bytes (offset where records start)
 * @param version SequenceFile format version
 * @param keyClassName Fully qualified class name of the key type
 * @param valueClassName Fully qualified class name of the value type
 * @param isCompressed Whether the file uses record-level compression
 * @param isBlockCompressed Whether the file uses block compression
 * @param compressionCodecClassName Optional compression codec class name
 * @param metadata Key-value metadata from the header
 */
case class SequenceFileHeader(
    syncMarker: Array[Byte],
    headerSize: Int,
    version: Int,
    keyClassName: String,
    valueClassName: String,
    isCompressed: Boolean,
    isBlockCompressed: Boolean,
    compressionCodecClassName: Option[String],
    metadata: Map[String, String]) {

  require(syncMarker.length == SequenceFileHeader.SYNC_SIZE,
    s"syncMarker must be ${SequenceFileHeader.SYNC_SIZE} bytes, got ${syncMarker.length}")

  /**
   * Whether this file can be parsed by the GPU native parser.
   * Currently only uncompressed files are supported.
   */
  def isGpuParseable: Boolean = !isCompressed && !isBlockCompressed
}

/**
 * Utility for parsing Hadoop SequenceFile headers.
 *
 * This parser reads only the header portion of a SequenceFile on the CPU,
 * extracting the sync marker and other metadata needed for GPU parsing.
 */
object SequenceFileHeader {
  /** Magic bytes at the start of every SequenceFile: "SEQ" */
  val MAGIC: Array[Byte] = Array('S'.toByte, 'E'.toByte, 'Q'.toByte)

  /** Current SequenceFile version (6) */
  val CURRENT_VERSION: Byte = 6

  /** Size of the sync marker */
  val SYNC_SIZE: Int = 16

  /**
   * Parse the header of a SequenceFile.
   *
   * @param path Path to the SequenceFile
   * @param conf Hadoop configuration
   * @return Parsed header information
   * @throws IllegalArgumentException if the file is not a valid SequenceFile
   */
  def parse(path: String, conf: Configuration): SequenceFileHeader = {
    parse(new Path(new URI(path)), conf)
  }

  /**
   * Parse the header of a SequenceFile.
   *
   * @param path Hadoop Path to the SequenceFile
   * @param conf Hadoop configuration
   * @return Parsed header information
   */
  def parse(path: Path, conf: Configuration): SequenceFileHeader = {
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(path)
    try {
      parseFromFSDataInputStream(fsin)
    } finally {
      fsin.close()
    }
  }

  /**
   * Parse the header from an FSDataInputStream.
   * Uses FSDataInputStream.getPos() for accurate position tracking.
   * Note: FSDataInputStream already extends DataInputStream, so we use it directly.
   *
   * @param fsin FSDataInputStream positioned at the start of the SequenceFile
   * @return Parsed header information
   */
  private def parseFromFSDataInputStream(fsin: FSDataInputStream): SequenceFileHeader = {
    // FSDataInputStream extends DataInputStream, use it directly without wrapping
    // This ensures getPos() accurately reflects what we've read

    // Read and verify magic
    val magic = new Array[Byte](MAGIC.length)
    fsin.readFully(magic)
    if (!java.util.Arrays.equals(magic, MAGIC)) {
      throw new IllegalArgumentException(
        s"Not a SequenceFile: invalid magic bytes. Expected 'SEQ', got '${new String(magic)}'")
    }

    // Read version
    val version = fsin.readByte()
    if (version > CURRENT_VERSION) {
      throw new VersionMismatchException(CURRENT_VERSION, version)
    }
    if (version < 5) {
      throw new IllegalArgumentException(
        s"SequenceFile version $version is not supported (minimum version 5)")
    }

    // Read key and value class names
    val keyClassName = Text.readString(fsin)
    val valueClassName = Text.readString(fsin)

    // Read compression flags (version >= 2)
    val isCompressed = fsin.readBoolean()

    // Read block compression flag (version >= 4)
    val isBlockCompressed = if (version >= 4) fsin.readBoolean() else false

    // Read compression codec (if compressed, version >= 5)
    val compressionCodecClassName = if (isCompressed) {
      Some(Text.readString(fsin))
    } else {
      None
    }

    // Read metadata (version >= 6)
    val metadata = if (version >= 6) {
      readMetadata(fsin)
    } else {
      Map.empty[String, String]
    }

    // Read sync marker
    val syncMarker = new Array[Byte](SYNC_SIZE)
    fsin.readFully(syncMarker)

    val headerSize = fsin.getPos.toInt

    SequenceFileHeader(
      syncMarker = syncMarker,
      headerSize = headerSize,
      version = version,
      keyClassName = keyClassName,
      valueClassName = valueClassName,
      isCompressed = isCompressed,
      isBlockCompressed = isBlockCompressed,
      compressionCodecClassName = compressionCodecClassName,
      metadata = metadata
    )
  }

  private def readMetadata(fsin: FSDataInputStream): Map[String, String] = {
    // Hadoop uses a 4-byte int for the metadata count (NOT VInt!)
    // See org.apache.hadoop.io.SequenceFile.Metadata.readFields()
    val numEntries = fsin.readInt()
    if (numEntries < 0) {
      throw new IllegalArgumentException(s"Invalid metadata entry count: $numEntries")
    }

    (0 until numEntries).map { _ =>
      val key = Text.readString(fsin)
      val value = Text.readString(fsin)
      (key, value)
    }.toMap
  }
}
