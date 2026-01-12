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

package com.nvidia.spark.rapids.spill

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream, IOException}

import com.nvidia.spark.rapids.HostAlloc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.GpuTaskMetrics

/**
 * Storage mode for SpillablePartialFileHandle.
 */
object PartialFileStorageMode extends Enumeration {
  val FILE_ONLY, MEMORY_WITH_SPILL = Value
}

/**
 * A specialized spillable handle for partial files that provides unified write/read
 * interfaces for both file-based and memory-based (with spill support) storage.
 *
 * This handle is designed for scenarios where:
 * 1. When memory is scarce (usage > threshold), use file-based storage directly
 * 2. When memory is sufficient, use host memory buffer with automatic spill support
 *
 * Features:
 * - Unified write/read interface regardless of storage mode
 * - Protection from spill during write phase
 * - Sequential read support to avoid frequent stream open/close
 * - Automatic transition from memory to disk when spilled
 * - Dynamic buffer expansion when capacity is exceeded (up to configured max limit)
 * - Automatic fallback to file when expansion fails or conditions not met
 *
 * @param storageMode Whether to use FILE_ONLY or MEMORY_WITH_SPILL
 * @param file File to use for FILE_ONLY mode or as spill target for MEMORY_WITH_SPILL
 * @param initialCapacity Initial capacity for buffer allocation (MEMORY_WITH_SPILL only)
 * @param maxBufferSize Maximum buffer size before spilling to disk
 * @param memoryThreshold Host memory usage threshold for buffer expansion decisions
 * @param priority Spill priority for memory-based mode
 * @param syncWrites Whether to force outstanding writes to disk
 */
class SpillablePartialFileHandle private (
    storageMode: PartialFileStorageMode.Value,
    file: File,
    initialCapacity: Long,
    maxBufferSize: Long,
    memoryThreshold: Double,
    priority: Long,
    syncWrites: Boolean)
  extends HostSpillableHandle[ai.rapids.cudf.HostMemoryBuffer] with Logging {

  // State management
  @volatile private var spilledToDisk: Boolean = false
  override private[spill] var host: Option[ai.rapids.cudf.HostMemoryBuffer] = None
  override val approxSizeInBytes: Long = initialCapacity
  
  // Track current buffer capacity (can grow via expansion)
  private var currentBufferCapacity: Long = initialCapacity

  // Protect from spill during write phase
  private var protectedFromSpill: Boolean = true
  private var writeFinished: Boolean = false

  // Write state
  private var writePosition: Long = 0L
  private var fileOutputStream: Option[FileOutputStream] = None
  private var bufferedOutputStream: Option[BufferedOutputStream] = None

  // Read state
  private var readPosition: Long = 0L
  private var fileInputStream: Option[FileInputStream] = None
  private var bufferedInputStream: Option[BufferedInputStream] = None
  private var totalBytesWritten: Long = 0L

  // Initialize host buffer for MEMORY_WITH_SPILL mode
  if (storageMode == PartialFileStorageMode.MEMORY_WITH_SPILL) {
    try {
      val buffer = ai.rapids.cudf.HostMemoryBuffer.allocate(initialCapacity)
      host = Some(buffer)
      currentBufferCapacity = initialCapacity
      this.taskPriority = priority
      SpillFramework.stores.hostStore.trackNoSpill(this)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to allocate initial buffer of $initialCapacity bytes, " +
          s"falling back to file-based storage", e)
        // Fallback to file-based if allocation fails
        spilledToDisk = true
        currentBufferCapacity = 0L
    }
  }

  /**
   * Check if we should use file for IO (either FILE_ONLY mode or spilled).
   */
  private def shouldUseFile: Boolean = {
    storageMode == PartialFileStorageMode.FILE_ONLY || spilledToDisk
  }

  /**
   * Expand host buffer capacity to meet required capacity.
   * Tries to double the capacity until reaching required size,
   * falls back to file-based if expansion fails.
   * 
   * Conditions checked before expansion:
   * 1. New capacity does not exceed configured max buffer size limit
   * 2. Current memory usage is below configured threshold
   * 
   * @param requiredCapacity The minimum capacity needed
   * @return true if successfully expanded, false if spilled to file instead
   */
  private def expandBuffer(requiredCapacity: Long): Boolean = {
    host match {
      case Some(currentBuffer) =>
        val oldCapacity = currentBufferCapacity
        
        // Calculate new capacity: keep doubling until >= requiredCapacity
        var newCapacity = oldCapacity
        while (newCapacity < requiredCapacity && newCapacity < maxBufferSize) {
          newCapacity = math.min(newCapacity * 2, maxBufferSize)
        }
        
        // Check if new capacity is still insufficient after expansion
        if (newCapacity < requiredCapacity) {
          logDebug(s"Buffer expansion cannot meet required capacity " +
            s"(need $requiredCapacity bytes, max limit is $maxBufferSize bytes), " +
            s"spilling to disk")
          spillBufferToFileAndSwitch(currentBuffer)
          return false
        }
        
        // Check if new capacity exceeds limit (should not happen due to math.min)
        if (newCapacity > maxBufferSize) {
          logDebug(s"Buffer expansion would exceed configured limit " +
            s"(need $newCapacity bytes, limit is $maxBufferSize bytes), spilling to disk")
          spillBufferToFileAndSwitch(currentBuffer)
          return false
        }
        
        // Check for Int.MaxValue limit due to ByteBuffer constraints
        if (newCapacity > Int.MaxValue) {
          logDebug(s"Buffer expansion would exceed ByteBuffer limit " +
            s"(need $newCapacity bytes, limit is ${Int.MaxValue} bytes), spilling to disk")
          spillBufferToFileAndSwitch(currentBuffer)
          return false
        }
        
        // Check if memory usage is still below threshold
        if (!HostAlloc.isUsageBelowThreshold(memoryThreshold)) {
          logDebug(s"Memory usage above ${memoryThreshold * 100}% threshold, " +
            s"spilling to disk instead of expanding")
          spillBufferToFileAndSwitch(currentBuffer)
          return false
        }
        
        try {
          // Allocate new larger buffer
          val newBuffer = ai.rapids.cudf.HostMemoryBuffer.allocate(newCapacity)
          try {
            // Copy existing data
            newBuffer.copyFromHostBuffer(0, currentBuffer, 0, writePosition)
            
            // Remove old buffer tracking and track new one
            SpillFramework.removeFromHostStore(this)
            currentBuffer.close()
            host = Some(newBuffer)
            currentBufferCapacity = newCapacity
            SpillFramework.stores.hostStore.trackNoSpill(this)
            
            logDebug(s"Expanded buffer from $oldCapacity to $newCapacity bytes " +
              s"(required $requiredCapacity bytes)")
            true
          } catch {
            case e: Exception =>
              newBuffer.close()
              throw e
          }
        } catch {
          case e: Exception =>
            logDebug(s"Failed to allocate buffer of $newCapacity bytes, " +
              s"spilling to disk", e)
            spillBufferToFileAndSwitch(currentBuffer)
            false
        }
      case None =>
        throw new IllegalStateException("Host buffer is null")
    }
  }

  /**
   * Spill current buffer content to file and switch to file-based mode.
   * Called when buffer expansion fails or capacity cannot grow further.
   */
  private def spillBufferToFileAndSwitch(
      buffer: ai.rapids.cudf.HostMemoryBuffer): Unit = {
    // Defensive check: writePosition should not exceed Int.MaxValue
    // because expandBuffer() limits buffer size to Int.MaxValue
    require(writePosition <= Int.MaxValue,
      s"Cannot spill buffer larger than Int.MaxValue: $writePosition bytes")
    
    // Write current buffer content to file
    val fos = new FileOutputStream(file)
    try {
      val channel = fos.getChannel
      val bb = buffer.asByteBuffer()
      bb.limit(writePosition.toInt)
      while (bb.hasRemaining) {
        channel.write(bb)
      }
      if (syncWrites) {
        channel.force(true)
      }
    } finally {
      fos.close()
    }

    // Release buffer and switch to file mode
    SpillFramework.removeFromHostStore(this)
    buffer.close()
    host = None
    spilledToDisk = true

    logDebug(s"Spilled buffer to ${file.getAbsolutePath} during write " +
      s"($writePosition bytes), continuing write to file")
  }

  /**
   * Write a single byte to the partial file.
   * No synchronization needed: write phase is protected from spilling.
   */
  def write(b: Int): Unit = {
    if (writeFinished) {
      throw new IllegalStateException("Write phase already finished")
    }

    if (shouldUseFile) {
      // FILE_ONLY mode or spilled: write to file
      ensureFileOutputStreamOpen()
      bufferedOutputStream.get.write(b)
      writePosition += 1
    } else {
      // MEMORY_WITH_SPILL mode: write to buffer (protected from spill)
      host match {
        case Some(_) =>
          // Check if buffer needs expansion
          val requiredCapacity = writePosition + 1
          if (requiredCapacity > currentBufferCapacity) {
            val expanded = expandBuffer(requiredCapacity)
            // After expansion, may have spilled to file, recursively call write
            if (!expanded) {
              // Spilled to file, retry write (will go to file branch)
              write(b)
              return
            }
          }
          // Write to buffer (may be new buffer after expansion)
          host.get.setByte(writePosition, b.toByte)
          writePosition += 1
        case None =>
          throw new IllegalStateException("Host buffer is null")
      }
    }
  }

  /**
   * Write bytes to the partial file.
   * No synchronization needed: write phase is protected from spilling.
   */
  def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    if (writeFinished) {
      throw new IllegalStateException("Write phase already finished")
    }

    if (shouldUseFile) {
      // FILE_ONLY mode or spilled: write to file
      ensureFileOutputStreamOpen()
      bufferedOutputStream.get.write(bytes, offset, length)
      writePosition += length
    } else {
      // MEMORY_WITH_SPILL mode: write to buffer (protected from spill)
      host match {
        case Some(_) =>
          // Check if buffer needs expansion
          val requiredCapacity = writePosition + length
          if (requiredCapacity > currentBufferCapacity) {
            logDebug(s"Buffer expansion needed: writePos=$writePosition, length=$length, " +
              s"required=$requiredCapacity, current=$currentBufferCapacity")
            val expanded = expandBuffer(requiredCapacity)
            // After expansion, may have spilled to file, recursively call write
            if (!expanded) {
              // Spilled to file, retry write (will go to file branch)
              write(bytes, offset, length)
              return
            }
            logDebug(s"After expansion: currentCapacity=$currentBufferCapacity, " +
              s"bufferLength=${host.get.getLength}")
          }
          // Write to buffer (may be new buffer after expansion)
          host.get.setBytes(writePosition, bytes, offset, length)
          writePosition += length
        case None =>
          throw new IllegalStateException("Host buffer is null")
      }
    }
  }

  /**
   * Finish write phase and enable spilling.
   * After this call, no more writes are allowed but reads can proceed.
   * 
   * This is where we record disk write savings metric: if this handle is in
   * MEMORY_WITH_SPILL mode and hasn't spilled yet, it means we successfully
   * avoided disk writes during the write phase.
   */
  def finishWrite(): Unit = {
    // Extract streams under lock, close them outside
    val (bos, fos, shouldRecordSavings) = synchronized {
      if (writeFinished) {
        return
      }

      writeFinished = true
      totalBytesWritten = writePosition
      protectedFromSpill = false

      // Check if we should record disk write savings:
      // 1. Must be MEMORY_WITH_SPILL mode (not FILE_ONLY)
      // 2. Must not have spilled yet (data still in memory)
      // This means we successfully avoided disk writes during write phase
      val recordSavings = storageMode == PartialFileStorageMode.MEMORY_WITH_SPILL &&
        !spilledToDisk && totalBytesWritten > 0

      val b = bufferedOutputStream
      val f = fileOutputStream
      bufferedOutputStream = None
      fileOutputStream = None
      (b, f, recordSavings)
    }

    // Close streams outside lock (IO operations can be slow)
    bos.foreach { s =>
      s.flush()
      s.close()
    }
    fos.foreach(_.close())
    
    // Record disk write savings if applicable
    if (shouldRecordSavings) {
      SpillablePartialFileHandle.recordDiskWriteSaved(totalBytesWritten)
      logDebug(s"Recorded disk write savings: $totalBytesWritten bytes " +
        s"(kept in memory during write phase)")
    }
  }

  /**
   * Read bytes from the partial file sequentially.
   * Returns number of bytes actually read, or -1 if EOF.
   * 
   * Note: This method is NOT thread-safe. Concurrent reads from multiple threads
   * are not supported. This class is designed for single-threaded sequential reads
   * in the shuffle merge phase (see RapidsShuffleInternalManagerBase.mergePartialFiles).
   * 
   * Internal synchronization only protects against concurrent spill operations,
   * not concurrent read operations.
   */
  def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    if (!writeFinished) {
      throw new IllegalStateException("Cannot read before write is finished")
    }

    if (readPosition >= totalBytesWritten) {
      return -1  // EOF
    }

    val actualLength = math.min(length, (totalBytesWritten - readPosition).toInt)

    def readFromFile(bytes: Array[Byte], offset: Int, length: Int): Int = {
      ensureFileInputStreamOpen()
      val bytesRead = bufferedInputStream.get.read(bytes, offset, length)
      if (bytesRead > 0) {
        readPosition += bytesRead
      }
      bytesRead
    }

    if (shouldUseFile) {
      // File-based: no spill can happen, no synchronization needed
      readFromFile(bytes, offset, actualLength)
    } else {
      // Memory-based: check volatile flag without lock
      if (spilledToDisk) {
        // Spilled after our first check, no lock needed now
        readFromFile(bytes, offset, actualLength)
      } else {
        // Still in memory, need lock for the read operation
        synchronized {
          // Double-check: may have spilled between our checks
          if (spilledToDisk) {
            // Just spilled, release lock and read from file
            // Note: We exit synchronized block here
          } else {
            // Confirmed still in memory, read with lock held
            host match {
              case Some(buffer) =>
                buffer.getBytes(bytes, offset, readPosition, actualLength)
                readPosition += actualLength
                return actualLength
              case None =>
                throw new IllegalStateException("Host buffer is null")
            }
          }
        }
        // If we reach here, it means spilled during double-check
        readFromFile(bytes, offset, actualLength)
      }
    }
  }

  /**
   * Get total bytes written to this partial file.
   */
  def getTotalBytesWritten: Long = totalBytesWritten

  /**
   * Check if this handle is using MEMORY_WITH_SPILL mode.
   */
  def isMemoryBased: Boolean = storageMode == PartialFileStorageMode.MEMORY_WITH_SPILL

  /**
   * Check if memory-based data has been spilled to disk.
   * Always returns false for FILE_ONLY mode.
   */
  def isSpilled: Boolean = spilledToDisk

  /**
   * Override spillable to add write phase protection.
   */
  override private[spill] def spillable: Boolean = synchronized {
    super.spillable && !protectedFromSpill
  }

  /**
   * Spill memory buffer to disk.
   */
  override def spill(): Long = synchronized {
    if (storageMode != PartialFileStorageMode.MEMORY_WITH_SPILL) {
      return 0L  // Nothing to spill for FILE_ONLY mode
    }

    if (!writeFinished) {
      // This should not happen because protectedFromSpill prevents spill during write
      logWarning("Attempted to spill during write phase, which should be protected")
      return 0L
    }

    host match {
      case Some(buffer) =>
        // Defensive check: totalBytesWritten should not exceed Int.MaxValue
        // because expandBuffer() limits buffer size to Int.MaxValue
        require(totalBytesWritten <= Int.MaxValue,
          s"Cannot spill buffer larger than Int.MaxValue: $totalBytesWritten bytes")
        
        // Spill all written data to file
        val fos = new FileOutputStream(file)
        try {
          val channel = fos.getChannel
          val bb = buffer.asByteBuffer()
          bb.limit(totalBytesWritten.toInt)
          while (bb.hasRemaining) {
            channel.write(bb)
          }
          if (syncWrites) {
            channel.force(true)
          }
        } finally {
          fos.close()
        }

        spilledToDisk = true
        SpillFramework.removeFromHostStore(this)
        buffer.close()
        host = None

        logDebug(s"Spilled to ${file.getAbsolutePath} " +
          s"($totalBytesWritten bytes)")

        totalBytesWritten

      case None =>
        0L  // Already spilled
    }
  }

  /**
   * Ensure file output stream is open for writing.
   * Thread-safe: uses synchronized to prevent duplicate stream creation.
   */
  private def ensureFileOutputStreamOpen(): Unit = synchronized {
    if (fileOutputStream.isEmpty) {
      val fos = new FileOutputStream(file, true)  // append mode
      fileOutputStream = Some(fos)
      bufferedOutputStream = Some(new BufferedOutputStream(fos, 64 * 1024))
    }
  }

  /**
   * Ensure file input stream is open for reading.
   * Thread-safe: uses synchronized to prevent duplicate stream creation.
   */
  private def ensureFileInputStreamOpen(): Unit = synchronized {
    if (fileInputStream.isEmpty) {
      val fis = new FileInputStream(file)
      // Skip to current read position
      if (readPosition > 0) {
        var remaining = readPosition
        while (remaining > 0) {
          val skipped = fis.skip(remaining)
          if (skipped <= 0) {
            throw new IOException(s"Failed to skip to position $readPosition")
          }
          remaining -= skipped
        }
      }
      fileInputStream = Some(fis)
      bufferedInputStream = Some(new BufferedInputStream(fis, 64 * 1024))
    }
  }

  /**
   * Close and cleanup resources.
   */
  override private[spill] def doClose(): Unit = synchronized {
    // Close output streams
    bufferedOutputStream.foreach { bos =>
      try { bos.close() } catch { case _: Exception => }
    }
    bufferedOutputStream = None
    fileOutputStream.foreach { fos =>
      try { fos.close() } catch { case _: Exception => }
    }
    fileOutputStream = None

    // Close input streams
    bufferedInputStream.foreach { bis =>
      try { bis.close() } catch { case _: Exception => }
    }
    bufferedInputStream = None
    fileInputStream.foreach { fis =>
      try { fis.close() } catch { case _: Exception => }
    }
    fileInputStream = None

    // Release host buffer (removes from SpillFramework tracking and closes buffer)
    releaseHostResource()

    // Delete file if it exists
    if (file != null && file.exists()) {
      try {
        file.delete()
      } catch {
        case _: Exception => // Ignore
      }
    }
  }
}

object SpillablePartialFileHandle extends Logging {
  
  /**
   * Record disk write savings for a SpillablePartialFileHandle.
   * Should be called when a handle successfully avoided disk writes during write phase.
   * 
   * This tracks bytes that were kept in memory during shuffle write phase,
   * avoiding disk writes compared to the baseline implementation.
   * 
   * @param bytesSaved Number of bytes that avoided disk write
   */
  private[spill] def recordDiskWriteSaved(bytesSaved: Long): Unit = {
    if (bytesSaved > 0) {
      GpuTaskMetrics.get.addDiskWriteSaved(bytesSaved)
      logDebug(s"Recorded disk write savings: $bytesSaved bytes " +
        s"(kept in memory during write phase)")
    }
  }
  
  /**
   * Create a file-only handle.
   * Data is written directly to disk without using host memory.
   *
   * @param file File to write data to
   * @param syncWrites Whether to force outstanding writes to disk
   */
  def createFileOnly(file: File, syncWrites: Boolean = false):
      SpillablePartialFileHandle = {
    new SpillablePartialFileHandle(
      storageMode = PartialFileStorageMode.FILE_ONLY,
      file = file,
      initialCapacity = 0L,
      maxBufferSize = 0L,
      memoryThreshold = 0.0,
      priority = Long.MinValue,
      syncWrites = syncWrites)
  }

  /**
   * Create a memory-with-spill handle.
   * Data is initially written to host memory buffer and can be spilled to disk
   * if needed. The buffer will automatically expand when needed (up to
   * maxBufferSize limit).
   *
   * @param initialCapacity Initial size of host memory buffer to allocate
   * @param maxBufferSize Maximum buffer size before spilling to disk
   * @param memoryThreshold Host memory usage threshold for buffer expansion
   *                        decisions
   * @param spillFile File to use when spilling is required
   * @param priority Spill priority
   * @param syncWrites Whether to force outstanding writes to disk
   */
  def createMemoryWithSpill(
      initialCapacity: Long,
      maxBufferSize: Long,
      memoryThreshold: Double,
      spillFile: File,
      priority: Long = Long.MinValue,
      syncWrites: Boolean = false): SpillablePartialFileHandle = {
    new SpillablePartialFileHandle(
      storageMode = PartialFileStorageMode.MEMORY_WITH_SPILL,
      file = spillFile,
      initialCapacity = initialCapacity,
      maxBufferSize = maxBufferSize,
      memoryThreshold = memoryThreshold,
      priority = priority,
      syncWrites = syncWrites)
  }
}

