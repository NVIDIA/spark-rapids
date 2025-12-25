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

package com.nvidia.spark.rapids.spill

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream, IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

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
  @volatile private var protectedFromSpill: Boolean = true
  @volatile private var writeFinished: Boolean = false

  // Write state
  private var writePosition: Long = 0L
  private var fileOutputStream: Option[FileOutputStream] = None
  private var bufferedOutputStream: Option[BufferedOutputStream] = None

  // Read state (for sequential read() method)
  private var readPosition: Long = 0L
  private var fileInputStream: Option[FileInputStream] = None
  private var bufferedInputStream: Option[BufferedInputStream] = None
  private var totalBytesWritten: Long = 0L

  // Random access read state (for concurrent readAt() method)
  private var randomAccessFile: Option[RandomAccessFile] = None
  private var fileChannel: Option[FileChannel] = None

  // Initialize host buffer for MEMORY_WITH_SPILL mode
  if (storageMode == PartialFileStorageMode.MEMORY_WITH_SPILL) {
    try {
      val buffer = ai.rapids.cudf.HostMemoryBuffer.allocate(initialCapacity, false)
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
        
        // Check if memory usage is still below threshold
        if (!HostAlloc.isUsageBelowThreshold(memoryThreshold)) {
          logDebug(s"Memory usage above ${memoryThreshold * 100}% threshold, " +
            s"spilling to disk instead of expanding")
          spillBufferToFileAndSwitch(currentBuffer)
          return false
        }
        
        try {
          // Allocate new larger buffer
          val newBuffer = ai.rapids.cudf.HostMemoryBuffer.allocate(newCapacity, false)
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
   */
  def finishWrite(): Unit = {
    // Extract streams under lock, close them outside
    val (bos, fos) = synchronized {
      if (writeFinished) {
        return
      }

      writeFinished = true
      totalBytesWritten = writePosition
      protectedFromSpill = false

      val b = bufferedOutputStream
      val f = fileOutputStream
      bufferedOutputStream = None
      fileOutputStream = None
      (b, f)
    }

    // Close streams outside lock (IO operations can be slow)
    bos.foreach { s =>
      s.flush()
      s.close()
    }
    fos.foreach(_.close())
  }

  /**
   * Read bytes from the partial file sequentially.
   * Returns number of bytes actually read, or -1 if EOF.
   * 
   * This method is designed for single-threaded sequential reading.
   * Synchronization is only needed to prevent concurrent spill from closing the buffer.
   */
  def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    if (!writeFinished) {
      throw new IllegalStateException("Cannot read before write is finished")
    }

    if (readPosition >= totalBytesWritten) {
      return -1  // EOF
    }

    val actualLength = math.min(length, (totalBytesWritten - readPosition).toInt)

    def readFromFile(): Int = {
      ensureFileInputStreamOpen()
      val bytesRead = bufferedInputStream.get.read(bytes, offset, actualLength)
      if (bytesRead > 0) {
        readPosition += bytesRead
      }
      bytesRead
    }

    // FILE_ONLY mode or already spilled: read from file directly
    if (shouldUseFile) {
      return readFromFile()
    }

    // MEMORY_WITH_SPILL mode: need to coordinate with spill()
    // Fast check without lock
    if (spilledToDisk) {
      return readFromFile()
    }

    // Try to read from memory buffer with lock protection
    synchronized {
      // Double-check after acquiring lock
      if (spilledToDisk) {
        // Spilled while waiting for lock, read from file instead
        // (file is guaranteed to be complete before spilledToDisk is set)
        return readFromFile()
      }

      // Still in memory, read with lock held to prevent concurrent spill
      host match {
        case Some(buffer) =>
          buffer.getBytes(bytes, offset, readPosition, actualLength)
          readPosition += actualLength
          actualLength
        case None =>
          throw new IllegalStateException(
            "Host buffer is null but spilledToDisk is false")
      }
    }
  }

  /**
   * Read bytes from a specific position without modifying internal read state.
   * Thread-safe for concurrent reads from different positions.
   * 
   * This method is designed for scenarios where multiple reducers need to
   * read different partitions from the same handle concurrently.
   *
   * @param position starting position to read from (0-based)
   * @param bytes destination buffer
   * @param offset offset in destination buffer
   * @param length number of bytes to read
   * @return number of bytes actually read, or -1 if position >= totalBytesWritten
   */
  def readAt(position: Long, bytes: Array[Byte], offset: Int, length: Int): Int = {
    if (!writeFinished) {
      throw new IllegalStateException("Cannot read before write is finished")
    }

    if (position < 0 || position >= totalBytesWritten) {
      return -1
    }

    val actualLength = math.min(length, (totalBytesWritten - position).toInt)
    if (actualLength <= 0) {
      return -1
    }

    def readFromFileChannel(): Int = {
      ensureFileChannelOpen()
      val buf = ByteBuffer.wrap(bytes, offset, actualLength)
      fileChannel.get.read(buf, position)
    }

    // FILE_ONLY mode or already spilled: read from file directly
    if (shouldUseFile) {
      return readFromFileChannel()
    }

    // MEMORY_WITH_SPILL mode: need to coordinate with spill()
    // Fast check without lock
    if (spilledToDisk) {
      return readFromFileChannel()
    }

    // Try to read from memory buffer with lock protection
    synchronized {
      // Double-check after acquiring lock
      if (spilledToDisk) {
        // Spilled while waiting for lock, read from file instead
        return readFromFileChannel()
      }

      // Still in memory, read with lock held to prevent concurrent spill
      host match {
        case Some(buffer) =>
          buffer.getBytes(bytes, offset, position, actualLength)
          actualLength
        case None =>
          throw new IllegalStateException(
            "Host buffer is null but spilledToDisk is false")
      }
    }
  }

  /**
   * Ensure FileChannel is open for random access reading.
   * Thread-safe: uses synchronized to prevent duplicate channel creation.
   */
  private def ensureFileChannelOpen(): Unit = synchronized {
    if (fileChannel.isEmpty) {
      val raf = new RandomAccessFile(file, "r")
      randomAccessFile = Some(raf)
      fileChannel = Some(raf.getChannel)
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
   * No lock needed: protectedFromSpill is volatile.
   */
  override private[spill] def spillable: Boolean = {
    super.spillable && !protectedFromSpill
  }

  // Flag to prevent concurrent spill attempts
  @volatile private var spillInProgress: Boolean = false

  /**
   * Spill memory buffer to disk.
   * 
   * IO operations are performed outside the synchronized block to allow
   * concurrent read() access to the buffer during the file write.
   */
  override def spill(): Long = {
    if (storageMode != PartialFileStorageMode.MEMORY_WITH_SPILL) {
      return 0L  // Nothing to spill for FILE_ONLY mode
    }

    // Fast path check without lock
    // writeFinished only transitions false->true, so no need to double-check in lock
    if (spilledToDisk || spillInProgress || !writeFinished) {
      return 0L
    }

    // Acquire buffer reference and set spilling flag under lock
    val bufferToSpill = synchronized {
      // Double check after acquiring lock (except writeFinished which only goes false->true)
      if (spilledToDisk || spillInProgress) {
        return 0L
      }

      host match {
        case Some(buffer) =>
          spillInProgress = true
          buffer
        case None =>
          return 0L  // Already spilled
      }
    }

    // Perform IO outside lock - read() can still access buffer during this time
    try {
      val fos = new FileOutputStream(file)
      try {
        val channel = fos.getChannel
        val bb = bufferToSpill.asByteBuffer()
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
    } catch {
      case e: Exception =>
        // IO failed, reset flag and propagate
        synchronized {
          spillInProgress = false
          notifyAll()  // Wake up any waiting doClose()
        }
        throw e
    }

    // Finalize spill under lock - now we close the buffer
    synchronized {
      spillInProgress = false
      notifyAll()  // Wake up any waiting doClose()

      // Check if doClose() already released the buffer while we were doing IO
      if (host.isEmpty) {
        // Buffer was already closed, nothing more to do
        return 0L
      }

      spilledToDisk = true
      SpillFramework.removeFromHostStore(this)
      bufferToSpill.close()
      host = None

      logDebug(s"Spilled to ${file.getAbsolutePath} " +
        s"($totalBytesWritten bytes)")

      totalBytesWritten
    }
  }

  /**
   * Ensure file output stream is open for writing.
   * No lock needed: write() is single-threaded by design.
   */
  private def ensureFileOutputStreamOpen(): Unit = {
    if (fileOutputStream.isEmpty) {
      val fos = new FileOutputStream(file, true)  // append mode
      fileOutputStream = Some(fos)
      bufferedOutputStream = Some(new BufferedOutputStream(fos, 64 * 1024))
    }
  }

  /**
   * Ensure file input stream is open for reading.
   * No lock needed: read() is single-threaded by design.
   */
  private def ensureFileInputStreamOpen(): Unit = {
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
   * This is where we record disk write savings: only if data was never spilled to disk
   * throughout the entire lifecycle (write phase + read phase), we count it as saved.
   */
  override private[spill] def doClose(): Unit = {
    // Collect resources to close under lock, then close them outside lock
    val (bos, fos, bis, fis, fc, raf) = synchronized {
      // Wait for any in-progress spill to complete before closing buffer
      while (spillInProgress) {
        wait()
      }

      // Record disk write savings if applicable:
      // 1. Must be MEMORY_WITH_SPILL mode (not FILE_ONLY)
      // 2. Must never have spilled to disk (data stayed in memory the entire time)
      // 3. Must have written some data
      if (storageMode == PartialFileStorageMode.MEMORY_WITH_SPILL &&
          !spilledToDisk && totalBytesWritten > 0) {
        SpillablePartialFileHandle.recordDiskWriteSaved(totalBytesWritten)
        logDebug(s"Recorded disk write savings: $totalBytesWritten bytes " +
          s"(data stayed in memory throughout lifecycle)")
      }

      // Collect streams/channels to close
      val result = (bufferedOutputStream, fileOutputStream,
        bufferedInputStream, fileInputStream, fileChannel, randomAccessFile)

      // Clear references
      bufferedOutputStream = None
      fileOutputStream = None
      bufferedInputStream = None
      fileInputStream = None
      fileChannel = None
      randomAccessFile = None

      // Release host buffer (removes from SpillFramework tracking and closes buffer)
      releaseHostResource()

      result
    }

    // Close streams outside lock (IO operations can be slow)
    bos.foreach { s => try { s.close() } catch { case _: Exception => } }
    fos.foreach { s => try { s.close() } catch { case _: Exception => } }
    bis.foreach { s => try { s.close() } catch { case _: Exception => } }
    fis.foreach { s => try { s.close() } catch { case _: Exception => } }
    fc.foreach { c => try { c.close() } catch { case _: Exception => } }
    raf.foreach { r => try { r.close() } catch { case _: Exception => } }

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
   * Should be called when a handle successfully avoided disk writes throughout its
   * entire lifecycle (from write to close without ever spilling to disk).
   * 
   * This tracks bytes that were kept in memory during the entire shuffle write/read
   * lifecycle, avoiding disk writes compared to the baseline implementation.
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

