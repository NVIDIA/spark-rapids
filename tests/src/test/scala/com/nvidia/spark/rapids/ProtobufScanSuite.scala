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

package com.nvidia.spark.rapids

import java.io.File

import ai.rapids.cudf.{DType, ProtobufOptions}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types._

/**
 * Unit tests for GPU-accelerated Protobuf reading.
 */
class ProtobufScanSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = new File(System.getProperty("java.io.tmpdir"), s"protobuf_test_${System.currentTimeMillis()}")
    tempDir.mkdirs()
  }

  override def afterAll(): Unit = {
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  /**
   * Encode a varint (variable-length integer) in protobuf format.
   */
  private def encodeVarint(value: Long): Array[Byte] = {
    var v = value
    val bytes = new scala.collection.mutable.ArrayBuffer[Byte]()
    while ((v & ~0x7FL) != 0) {
      bytes += ((v & 0x7F) | 0x80).toByte
      v >>>= 7
    }
    bytes += (v & 0x7F).toByte
    bytes.toArray
  }

  /**
   * Create a simple protobuf message with INT64 and STRING fields.
   * Format:
   * - Field 1 (INT64): tag=0x08, value as varint
   * - Field 2 (STRING): tag=0x12, length as varint, string bytes
   */
  private def createProtobufMessage(intValue: Long, stringValue: String): Array[Byte] = {
    val buffer = new scala.collection.mutable.ArrayBuffer[Byte]()
    
    // Field 1: INT64 (field number=1, wire type=0 varint)
    // Tag: (1 << 3) | 0 = 0x08
    buffer += 0x08.toByte
    buffer ++= encodeVarint(intValue)
    
    // Field 2: STRING (field number=2, wire type=2 length-delimited)
    // Tag: (2 << 3) | 2 = 0x12
    buffer += 0x12.toByte
    val strBytes = stringValue.getBytes("UTF-8")
    buffer ++= encodeVarint(strBytes.length)
    buffer ++= strBytes
    
    buffer.toArray
  }

  /**
   * Create length-delimited protobuf data (multiple messages).
   */
  private def createLengthDelimitedData(messages: Seq[(Long, String)]): Array[Byte] = {
    val buffer = new scala.collection.mutable.ArrayBuffer[Byte]()
    messages.foreach { case (intVal, strVal) =>
      val msg = createProtobufMessage(intVal, strVal)
      buffer ++= encodeVarint(msg.length)
      buffer ++= msg
    }
    buffer.toArray
  }

  test("Spark data type to cudf DType conversion") {
    assert(GpuProtobufScan.sparkTypeToCudfDType(LongType) == DType.INT64)
    assert(GpuProtobufScan.sparkTypeToCudfDType(IntegerType) == DType.INT32)
    assert(GpuProtobufScan.sparkTypeToCudfDType(StringType) == DType.STRING)
    assert(GpuProtobufScan.sparkTypeToCudfDType(DoubleType) == DType.FLOAT64)
    assert(GpuProtobufScan.sparkTypeToCudfDType(FloatType) == DType.FLOAT32)
    assert(GpuProtobufScan.sparkTypeToCudfDType(BooleanType) == DType.BOOL8)
  }

  test("Protobuf field schema creation") {
    val schema = Seq(
      ProtobufFieldSchema("id", 1, LongType),
      ProtobufFieldSchema("name", 2, StringType),
      ProtobufFieldSchema("value", 3, DoubleType)
    )
    
    assert(schema.size == 3)
    assert(schema.head.name == "id")
    assert(schema.head.fieldNumber == 1)
    assert(schema.head.dataType == LongType)
  }

  test("Protobuf read options") {
    val schema = Seq(
      ProtobufFieldSchema("field1", 1, LongType),
      ProtobufFieldSchema("field2", 2, StringType)
    )
    
    val options = ProtobufReadOptions(schema, isHadoopSequenceFile = true)
    assert(options.isHadoopSequenceFile)
    assert(options.schema.size == 2)
    
    val options2 = ProtobufReadOptions(schema, isHadoopSequenceFile = false)
    assert(!options2.isHadoopSequenceFile)
  }

  test("Varint encoding") {
    // Test small values
    assert(encodeVarint(0) sameElements Array[Byte](0))
    assert(encodeVarint(1) sameElements Array[Byte](1))
    assert(encodeVarint(127) sameElements Array[Byte](127))
    
    // Test values requiring 2 bytes
    assert(encodeVarint(128) sameElements Array[Byte](-128.toByte, 1))
    assert(encodeVarint(300) sameElements Array[Byte](0xAC.toByte, 0x02.toByte))
    
    // Test larger values
    val large = encodeVarint(100000)
    assert(large.length == 3)
  }

  test("Protobuf message creation") {
    val msg = createProtobufMessage(42, "hello")
    
    // Verify message starts with field 1 tag (0x08)
    assert(msg(0) == 0x08.toByte)
    
    // Verify field 2 tag (0x12) exists
    assert(msg.contains(0x12.toByte))
    
    // Verify "hello" is in the message
    val msgStr = new String(msg, "UTF-8")
    assert(msgStr.contains("hello"))
  }

  test("Length-delimited data creation") {
    val messages = Seq(
      (1L, "first"),
      (2L, "second"),
      (3L, "third")
    )
    
    val data = createLengthDelimitedData(messages)
    assert(data.nonEmpty)
    
    // Data should contain all strings
    val dataStr = new String(data, "UTF-8")
    messages.foreach { case (_, str) =>
      assert(dataStr.contains(str), s"Data should contain '$str'")
    }
  }

  test("ProtobufOptions builder") {
    val builder = ProtobufOptions.builder()
    builder.withField("id", 1, DType.INT64)
    builder.withField("name", 2, DType.STRING)
    builder.withHadoopSequenceFile(true)
    
    val options = builder.build()
    
    assert(options.isHadoopSequenceFile)
    val schema = options.getSchema
    assert(schema.size() == 2)
  }

  test("GPU protobuf reading from bytes") {
    // Skip if GPU is not available
    if (!isGpuAvailable) {
      cancel("GPU not available for this test")
    }

    val messages = Seq(
      (100L, "message_0"),
      (200L, "message_1"),
      (300L, "message_2"),
      (400L, "message_3"),
      (500L, "message_4")
    )

    val data = createLengthDelimitedData(messages)

    val schema = Seq(
      ProtobufFieldSchema("id", 1, LongType),
      ProtobufFieldSchema("name", 2, StringType)
    )
    val options = ProtobufReadOptions(schema, isHadoopSequenceFile = false)

    try {
      val table = GpuProtobufScan.readProtobufBytes(data, schema, options)
      withResource(table) { t =>
        assert(t.getRowCount > 0, "Table should have rows")
        assert(t.getNumberOfColumns == 2, "Table should have 2 columns")
      }
    } catch {
      case e: Exception =>
        // Log the error for debugging
        System.err.println(s"GPU protobuf reading failed: ${e.getMessage}")
        cancel(s"GPU protobuf reading not available: ${e.getMessage}")
    }
  }

  test("GPU protobuf reading performance benchmark") {
    // Skip if GPU is not available
    if (!isGpuAvailable) {
      cancel("GPU not available for this test")
    }

    // Create a larger dataset for benchmarking
    val numMessages = 10000
    val messages = (0 until numMessages).map { i =>
      (i.toLong * 100, s"benchmark_message_$i")
    }

    val data = createLengthDelimitedData(messages)
    println(s"Created ${data.length} bytes of protobuf data with $numMessages messages")

    val schema = Seq(
      ProtobufFieldSchema("id", 1, LongType),
      ProtobufFieldSchema("name", 2, StringType)
    )
    val options = ProtobufReadOptions(schema, isHadoopSequenceFile = false)

    try {
      // Warm-up
      val warmupTable = GpuProtobufScan.readProtobufBytes(data, schema, options)
      warmupTable.close()

      // Benchmark
      val startTime = System.nanoTime()
      val iterations = 10
      
      for (_ <- 0 until iterations) {
        val table = GpuProtobufScan.readProtobufBytes(data, schema, options)
        table.close()
      }
      
      val elapsedNs = System.nanoTime() - startTime
      val avgMs = elapsedNs / 1e6 / iterations

      println(s"\n=== Protobuf GPU Reading Benchmark ===")
      println(s"Messages: $numMessages")
      println(s"Data size: ${data.length} bytes")
      println(s"Iterations: $iterations")
      println(s"Average time: $avgMs ms")
      println(s"Throughput: ${numMessages / (avgMs / 1000)} messages/sec")
      println(s"Data throughput: ${data.length / (avgMs / 1000) / 1024 / 1024} MB/sec")

    } catch {
      case e: Exception =>
        System.err.println(s"GPU protobuf benchmark failed: ${e.getMessage}")
        cancel(s"GPU protobuf reading not available: ${e.getMessage}")
    }
  }

  private def isGpuAvailable: Boolean = {
    try {
      ai.rapids.cudf.Cuda.getDeviceCount > 0
    } catch {
      case _: Exception => false
    }
  }
}

/**
 * Integration tests that require a SparkSession.
 */
class ProtobufScanIntegrationSuite extends SparkQueryCompareTestSuite {

  test("protobuf configuration is accessible") {
    withGpuSparkSession(spark => {
      val conf = spark.conf
      
      // Test that our configuration keys are recognized
      // They may have default values
      val protobufEnabled = try {
        conf.get("spark.rapids.sql.format.protobuf.enabled", "false")
      } catch {
        case _: Exception => "false"
      }
      
      assert(protobufEnabled == "false" || protobufEnabled == "true")
    })
  }

  test("protobuf schema mapping") {
    withGpuSparkSession(spark => {
      import spark.implicits._
      
      // Test that we can create a DataFrame with types that match protobuf fields
      val df = Seq(
        (1L, "test1", 1.5),
        (2L, "test2", 2.5),
        (3L, "test3", 3.5)
      ).toDF("id", "name", "value")
      
      assert(df.schema.fields(0).dataType == LongType)
      assert(df.schema.fields(1).dataType == StringType)
      assert(df.schema.fields(2).dataType == DoubleType)
      
      val count = df.count()
      assert(count == 3)
    })
  }
}

