/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "321"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import java.io.{DataInputStream, File, FileInputStream, IOException, ObjectStreamException}
import java.util.UUID
import java.util.zip.CheckedInputStream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{HashPartitioner, SparkConf, SparkException, TaskContext}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.sql.rapids.shims.RapidsShuffleThreadedWriter
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockManager, DiskBlockObjectWriter, ShuffleChecksumBlockId, ShuffleDataBlockId, ShuffleIndexBlockId, TempShuffleBlockId}
import org.apache.spark.util.Utils


/**
 * This is mostly ported from Apache Spark's BypassMergeSortShuffleWriterSuite
 * It is only targetted for Spark 3.2.0+ but it touches all of the code for Spark 3.1.2
 * as well, except for some small tweaks when committing with checksum support.
 */
trait ShuffleChecksumTestHelper {

  /**
   * Ensure that the checksum values are consistent between write and read side.
   */
  def compareChecksums(numPartition: Int,
                       algorithm: String,
                       checksum: File,
                       data: File,
                       index: File): Unit = {
    assert(checksum.exists(), "Checksum file doesn't exist")
    assert(data.exists(), "Data file doesn't exist")
    assert(index.exists(), "Index file doesn't exist")

    var checksumIn: DataInputStream = null
    val expectChecksums = Array.ofDim[Long](numPartition)
    try {
      checksumIn = new DataInputStream(new FileInputStream(checksum))
      (0 until numPartition).foreach(i => expectChecksums(i) = checksumIn.readLong())
    } finally {
      if (checksumIn != null) {
        checksumIn.close()
      }
    }

    var dataIn: FileInputStream = null
    var indexIn: DataInputStream = null
    var checkedIn: CheckedInputStream = null
    try {
      dataIn = new FileInputStream(data)
      indexIn = new DataInputStream(new FileInputStream(index))
      var prevOffset = indexIn.readLong
      (0 until numPartition).foreach { i =>
        val curOffset = indexIn.readLong
        val limit = (curOffset - prevOffset).toInt
        val bytes = new Array[Byte](limit)
        val checksumCal = ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm)
        checkedIn = new CheckedInputStream(
          new LimitedInputStream(dataIn, curOffset - prevOffset), checksumCal)
        checkedIn.read(bytes, 0, limit)
        prevOffset = curOffset
        // checksum must be consistent at both write and read sides
        assert(checkedIn.getChecksum.getValue == expectChecksums(i))
      }
    } finally {
      if (dataIn != null) {
        dataIn.close()
      }
      if (indexIn != null) {
        indexIn.close()
      }
      if (checkedIn != null) {
        checkedIn.close()
      }
    }
  }
}

class BadSerializable(i: Int) extends Serializable {
  @throws(classOf[ObjectStreamException])
  def writeReplace(): Object = {
    if (i >= 500) {
      throw new IOException(s"failed to write $i")
    }
    this
  }
}

// Added so that we can mock a method that was created in Spark 3.3.0
// and the override can be used in all versions of Spark
trait ShimIndexShuffleBlockResolver330 {
  def createTempFile(file: File): File
}
class TestIndexShuffleBlockResolver(
    conf: SparkConf,
    bm: BlockManager)
      extends IndexShuffleBlockResolver(conf, bm)
        with ShimIndexShuffleBlockResolver330 {
  // implemented in Spark 3.3.0
  override def createTempFile(file: File): File = { null }
}

class RapidsShuffleThreadedWriterSuite extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MockitoSugar
    with ShuffleChecksumTestHelper
    with Logging {
  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: TestIndexShuffleBlockResolver = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS)
    private var dependency: GpuShuffleDependency[Int, Int, Int] = _

  @scala.annotation.nowarn("msg=consider using immutable val")
  @Mock(answer = RETURNS_SMART_NULLS)
    private var dependencyBad: GpuShuffleDependency[Int, BadSerializable, BadSerializable] = _

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _
  private var shuffleExecutorComponents: ShuffleExecutorComponents = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)
    .set("spark.app.id", "sampleApp")
  private val temporaryFilesCreated: mutable.Buffer[File] = new ArrayBuffer[File]()
  private val blockIdToFileMap: mutable.Map[BlockId, File] = new mutable.HashMap[BlockId, File]
  private var shuffleHandle: ShuffleHandleWithMetrics[Int, Int, Int] = _

  private val numWriterThreads = 2

  override def beforeAll(): Unit = {
    RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(numWriterThreads, 0)
  }

  override def afterAll(): Unit = {
    RapidsShuffleInternalManagerBase.stopThreadPool()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    TaskContext.setTaskContext(taskContext)
    MockitoAnnotations.openMocks(this).close()
    tempDir = Utils.createTempDir()
    outputFile = File.createTempFile("shuffle", null, tempDir)
    taskMetrics = spy(new TaskMetrics)
    val shuffleWriteMetrics = new ShuffleWriteMetrics
    shuffleHandle = new ShuffleHandleWithMetrics[Int, Int, Int](
      0, Map.empty, dependency)
    when(dependency.partitioner).thenReturn(new HashPartitioner(7))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependencyBad.partitioner).thenReturn(new HashPartitioner(7))
    when(dependencyBad.serializer).thenReturn(new JavaSerializer(conf))
    when(taskMetrics.shuffleWriteMetrics).thenReturn(shuffleWriteMetrics)
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)

    when(blockResolver.writeMetadataFileAndCommit(
      anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[Array[Long]]), any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val tmp = invocationOnMock.getArguments()(4).asInstanceOf[File]
        if (tmp != null) {
          outputFile.delete
          tmp.renameTo(outputFile)
        }
        null
      }

    when(blockManager.getDiskWriter(
      any[BlockId],
      any[File],
      any[SerializerInstance],
      anyInt(),
      any[ShuffleWriteMetrics]))
      .thenAnswer { invocation =>
        val args = invocation.getArguments
        val manager = new SerializerManager(new JavaSerializer(conf), conf)
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File],
          manager,
          args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[Int],
          syncWrites = false,
          args(4).asInstanceOf[ShuffleWriteMetrics],
          blockId = args(0).asInstanceOf[BlockId])
      }

    when(blockResolver.createTempFile(any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val file = invocationOnMock.getArguments()(0).asInstanceOf[File]
        Utils.tempFileWith(file)
      }

    when(diskBlockManager.createTempShuffleBlock())
      .thenAnswer { _ =>
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        blockIdToFileMap.put(blockId, file)
        temporaryFilesCreated += file
        (blockId, file)
      }

    when(diskBlockManager.getFile(any[BlockId])).thenAnswer { invocation =>
      blockIdToFileMap(invocation.getArguments.head.asInstanceOf[BlockId])
    }

    shuffleExecutorComponents = new LocalDiskShuffleExecutorComponents(
      conf, blockManager, blockResolver)
  }

  override def afterEach(): Unit = {
    TaskContext.unset()
    blockIdToFileMap.clear()
    temporaryFilesCreated.clear()
    try {
      Utils.deleteRecursively(tempDir)
    } catch {
      case NonFatal(e) =>
        // Catch non-fatal errors here as we are cleaning up directories using a Spark utility
        // and we shouldn't fail a test for these exceptions. See:
        // https://github.com/NVIDIA/spark-rapids/issues/6515
        logWarning(s"Error while cleaning up $tempDir", e)
    } finally {
      super.afterEach()
    }
  }

  test("write empty iterator") {
    val writer = new RapidsShuffleThreadedWriter[Int, Int](
      blockManager,
      shuffleHandle,
      0L, // MapId
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      1024 * 1024,
      shuffleExecutorComponents,
      numWriterThreads)
    writer.write(Iterator.empty)
    writer.stop( /* success = */ true)
    assert(writer.getPartitionLengths.sum === 0)
    assert(writer.getBytesInFlight == 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    assert(temporaryFilesCreated.isEmpty)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  Seq(true, false).foreach { transferTo =>
    test(s"write with some empty partitions - transferTo $transferTo") {
      val transferConf = conf.clone.set("spark.file.transferTo", transferTo.toString)
      def records: Iterator[(Int, Int)] =
        Iterator((1, 1), (5, 5)) ++ (0 until 100000).iterator.map(x => (2, 2))
      val writer = new RapidsShuffleThreadedWriter[Int, Int](
        blockManager,
        shuffleHandle,
        0L, // MapId
        transferConf,
        new ThreadSafeShuffleWriteMetricsReporter(taskContext.taskMetrics().shuffleWriteMetrics),
        1024 * 1024,
        shuffleExecutorComponents,
        numWriterThreads)
      writer.write(records)
      writer.stop( /* success = */ true)
      assert(temporaryFilesCreated.nonEmpty)
      assert(writer.getPartitionLengths.sum === outputFile.length())
      assert(writer.getPartitionLengths.count(_ == 0L) === 4) // should be 4 zero length files
      assert(writer.getBytesInFlight == 0)
      assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temp files were deleted
      val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
      assert(shuffleWriteMetrics.bytesWritten === outputFile.length())
      assert(shuffleWriteMetrics.recordsWritten === records.length)
      assert(taskMetrics.diskBytesSpilled === 0)
      assert(taskMetrics.memoryBytesSpilled === 0)
    }
  }

  test("only generate temp shuffle file for non-empty partition") {
    // Using exception to test whether only non-empty partition creates temp shuffle file,
    // because temp shuffle file will only be cleaned after calling stop(false) in the failure
    // case, so we could use it to validate the temp shuffle files.
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++
        (0 until 100000).iterator.map { i =>
          if (i == 99990) {
            throw new SparkException("intentional failure")
          } else {
            (2, 2)
          }
        }

    val writer = new RapidsShuffleThreadedWriter[Int, Int](
      blockManager,
      shuffleHandle,
      0L, // MapId
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      1024 * 1024,
      shuffleExecutorComponents,
      numWriterThreads)

    intercept[SparkException] {
      writer.write(records)
    }

    assert(temporaryFilesCreated.nonEmpty)
    // Only 3 temp shuffle files will be created
    assert(temporaryFilesCreated.count(_.exists()) === 3)

    writer.stop( /* success = */ false)
    assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temporary files were deleted
    assert(writer.getBytesInFlight == 0)
  }

  test("cleanup of intermediate files after errors") {
    val writer = new RapidsShuffleThreadedWriter[Int, Int](
      blockManager,
      shuffleHandle,
      0L, // MapId
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      1024 * 1024,
      shuffleExecutorComponents,
      numWriterThreads)
    intercept[SparkException] {
      writer.write((0 until 100000).iterator.map(i => {
        if (i == 99990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    }
    assert(temporaryFilesCreated.nonEmpty)
    writer.stop( /* success = */ false)
    assert(temporaryFilesCreated.count(_.exists()) === 0)
    assert(writer.getBytesInFlight == 0)
  }

  test("write checksum file") {
    // this is a spy so we can intercept calls to `createTempShuffleBlock`
    // in spark 3.3.0+
    val blockResolver = spy(new TestIndexShuffleBlockResolver(conf, blockManager))
    val shuffleId = shuffleHandle.shuffleId
    val mapId = 0
    val checksumBlockId = ShuffleChecksumBlockId(shuffleId, mapId, 0)
    val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, 0)
    val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, 0)
    val checksumAlgorithm = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)
    val checksumFileName = ShuffleChecksumHelper.getChecksumFileName(
      checksumBlockId.name, checksumAlgorithm)
    val checksumFile = new File(tempDir, checksumFileName)
    val dataFile = new File(tempDir, dataBlockId.name)
    val indexFile = new File(tempDir, indexBlockId.name)
    reset(diskBlockManager)
    when(diskBlockManager.getFile(checksumFileName)).thenAnswer(_ => checksumFile)
    when(diskBlockManager.getFile(dataBlockId)).thenAnswer(_ => dataFile)
    when(diskBlockManager.getFile(indexBlockId)).thenAnswer(_ => indexFile)
    when(diskBlockManager.createTempShuffleBlock())
      .thenAnswer { _ =>
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        temporaryFilesCreated += file
        (blockId, file)
      }

    when(blockResolver.createTempFile(any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val file = invocationOnMock.getArguments()(0).asInstanceOf[File]
        Utils.tempFileWith(file)
      }

    val numPartition = shuffleHandle.dependency.partitioner.numPartitions
    val writer = new RapidsShuffleThreadedWriter[Int, Int](
      blockManager,
      shuffleHandle,
      mapId,
      conf,
      new ThreadSafeShuffleWriteMetricsReporter(taskContext.taskMetrics().shuffleWriteMetrics),
      1024 * 1024,
      new LocalDiskShuffleExecutorComponents(conf, blockManager, blockResolver),
      numWriterThreads)

    writer.write(Iterator((0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))
    writer.stop( /* success = */ true)
    assert(writer.getBytesInFlight == 0)
    assert(checksumFile.exists())
    assert(checksumFile.length() === 8 * numPartition)
    compareChecksums(numPartition, checksumAlgorithm, checksumFile, dataFile, indexFile)
  }

  Seq(true, false).foreach { stopWithSuccess =>
    test(s"create an exception in one of the writers and stop with success = $stopWithSuccess") {
      def records: Iterator[(Int, BadSerializable)] =
        Iterator(
          (1, new BadSerializable(1)),
          (5, new BadSerializable(5))) ++
          (10 until 100000).iterator.map(x => (2, new BadSerializable(x)))

      val shuffleHandle = new ShuffleHandleWithMetrics[Int, BadSerializable, BadSerializable](
        0,
        Map.empty,
        dependencyBad
      )
      val writer = new RapidsShuffleThreadedWriter[Int, BadSerializable](
        blockManager,
        shuffleHandle,
        0L, // MapId
        conf,
        new ThreadSafeShuffleWriteMetricsReporter(taskContext.taskMetrics().shuffleWriteMetrics),
        1024 * 1024,
        shuffleExecutorComponents,
        numWriterThreads)
      assertThrows[IOException] {
        writer.write(records)
      }
      if (stopWithSuccess) {
        assertThrows[IllegalStateException] {
          writer.stop(true)
        }
      } else {
        writer.stop(false)
      }
      assert(temporaryFilesCreated.nonEmpty)
      assert(writer.getPartitionLengths == null)
      assert(writer.getBytesInFlight == 0)
      assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temp files were deleted
    }
  }
}

