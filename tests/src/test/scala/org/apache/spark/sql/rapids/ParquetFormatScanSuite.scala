/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.File

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.duration._

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.scalatest.concurrent.Eventually

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.Utils

/**
 * This is intended to give us a place to verify that we can cover all
 * of the allowed parquet formats as described in
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 * A lot of this testing code is based off of similar Spark tests.
 */
class ParquetFormatScanSuite extends SparkQueryCompareTestSuite with Eventually {
  implicit class RecordConsumerDSL(consumer: RecordConsumer) {
    def message(f: => Unit): Unit = {
      consumer.startMessage()
      f
      consumer.endMessage()
    }

    def group(f: => Unit): Unit = {
      consumer.startGroup()
      f
      consumer.endGroup()
    }

    def field(name: String, index: Int)(f: => Unit): Unit = {
      consumer.startField(name, index)
      f
      consumer.endField(name, index)
    }
  }

  /**
   * Waits for all tasks on all executors to be finished.
   */
  protected def waitForTasksToFinish(spark: SparkSession): Unit = {
    eventually(timeout(10.seconds)) {
      assert(spark.sparkContext.statusTracker
          .getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withTempDir(spark: SparkSession)(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try {
      f(dir)
      waitForTasksToFinish(spark)
    } finally {
      Utils.deleteRecursively(dir)
    }
  }

  /**
   * A testing Parquet [[WriteSupport]] implementation used to write manually constructed Parquet
   * records with arbitrary structures.
   */
  private class DirectWriteSupport(schema: MessageType, metadata: Map[String, String])
      extends WriteSupport[RecordConsumer => Unit] {

    private var recordConsumer: RecordConsumer = _

    override def init(configuration: Configuration): WriteContext = {
      new WriteContext(schema, metadata.asJava)
    }

    override def write(recordWriter: RecordConsumer => Unit): Unit = {
      recordWriter.apply(recordConsumer)
    }

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
      this.recordConsumer = recordConsumer
    }
  }

  /**
   * Writes arbitrary messages conforming to a given `schema` to a Parquet file located by `path`.
   * Records are produced by `recordWriters`.
   */
  def writeDirect(path: String, schema: String, recordWriters: (RecordConsumer => Unit)*): Unit = {
    writeDirect(path, schema, Map.empty[String, String], recordWriters: _*)
  }

  /**
   * Writes arbitrary messages conforming to a given `schema` to a Parquet file located by `path`
   * with given user-defined key-value `metadata`. Records are produced by `recordWriters`.
   */
  def writeDirect(
      path: String,
      schema: String,
      metadata: Map[String, String],
      recordWriters: (RecordConsumer => Unit)*): Unit = {
    val messageType = MessageTypeParser.parseMessageType(schema)
    val testWriteSupport = new DirectWriteSupport(messageType, metadata)
    /**
     * Provide a builder for constructing a parquet writer - after PARQUET-248 directly constructing
     * the writer is deprecated and should be done through a builder. The default builders include
     * Avro - but for raw Parquet writing we must create our own builder.
     */
    class ParquetWriterBuilder() extends
        ParquetWriter.Builder[RecordConsumer => Unit, ParquetWriterBuilder](new Path(path)) {
      override def getWriteSupport(conf: Configuration) = testWriteSupport
      override def self() = this
    }
    val parquetWriter = new ParquetWriterBuilder().build()
    try recordWriters.foreach(parquetWriter.write) finally parquetWriter.close()
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
    answer.map(prepareRow)
  }

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    case (a: Float, b: Float) =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a, b) => a == b
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row]): Unit = {
    assert(compare(prepareAnswer(expectedAnswer), prepareAnswer(sparkAnswer)))
  }

  test("STRING") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required BINARY str_test (UTF8);
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/STRING_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("str_test", 0) {
              rc.addBinary(Binary.fromString("TEST"))
            }
          }
        }, { rc =>
          rc.message {
            rc.field("str_test", 0) {
              rc.addBinary(Binary.fromString("STRING"))
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row("TEST"), Row("STRING")), data)
      }
    })
  }

  test("ENUM") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required BINARY enum_test (ENUM);
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/ENUM_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("enum_test", 0) {
              rc.addBinary(Binary.fromString("A"))
            }
          }
        }, { rc =>
          rc.message {
            rc.field("enum_test", 0) {
              rc.addBinary(Binary.fromString("B"))
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row("A"), Row("B")), data)
      }
    })
  }

//  // The java code on the CPU does not support UUID at all, so this test is not working, or fully
//  // done
//  test("UUID") {
//    withCpuSparkSession(spark => {
//      val schema =
//        """message spark {
//          |  required fixed_len_byte_array(16) uuid_test (UUID),
//          |}
//        """.stripMargin
//
//      withTempDir(spark) { dir =>
//        val testPath = dir + "/UUID_TEST.parquet"
//        writeDirect(testPath, schema, { rc =>
//          rc.message {
//            rc.field("uuid_test", 0) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
//                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A)))
//            }
//          }
//        })
//
//        val data = spark.read.parquet(testPath).collect()
//        // TODO not sure
//        //sameRows(Seq(Row("A")), data)
//      }
//    })
//  }

  // TODO should we test overflow cases for INT_8 etc. It is implementation specific what
  //  to do in those cases but we probably want to match what spark does....
  test("int32") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required int32 byte_test (INT_8);
          |  required int32 short_test (INT_16);
          |  required int32 int_test;
          |  required int32 int_test2 (INT_32);
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/INT32_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("byte_test", 0) {
              rc.addInteger(22)
            }
            rc.field("short_test", 1) {
              rc.addInteger(300)
            }
            rc.field("int_test", 2) {
              rc.addInteger(1)
            }
            rc.field("int_test2", 3) {
              rc.addInteger(3)
            }
          }
        }, { rc =>
          rc.message {
            rc.field("byte_test", 0) {
              rc.addInteger(-21)
            }
            rc.field("short_test", 1) {
              rc.addInteger(-301)
            }
            rc.field("int_test", 2) {
              rc.addInteger(-2)
            }
            rc.field("int_test2", 3) {
              rc.addInteger(-4)
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(22, 300, 1, 3),
          Row(-21, -301, -2, -4)), data)
      }
    })
  }

  test("int32 - unsigned") {
    withGpuSparkSession(spark => {
      // TODO the UINT is not supported on older version of spark. Need to figure out where it is
      //  supported so the test works properly
      val schema =
      """message spark {
        |  required int32 ubyte_test (UINT_8);
        |  required int32 ushort_test (UINT_16);
        |  required int32 uint_test (UINT_32);
        |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/UINT32_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("ubyte_test", 0) {
              rc.addInteger(22)
            }
            rc.field("ushort_test", 1) {
              rc.addInteger(300)
            }
            rc.field("uint_test", 2) {
              rc.addInteger(1)
            }
          }
        }, { rc =>
          rc.message {
            rc.field("ubyte_test", 0) {
              rc.addInteger(21)
            }
            rc.field("ushort_test", 1) {
              rc.addInteger(301)
            }
            rc.field("uint_test", 2) {
              rc.addInteger(2)
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(22, 300, 1),
          Row(21, 301, 2)), data)
      }
    })
  }

  test("int64") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required int64 int_test;
          |  required int64 int_test2 (INT_64);
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/INT64_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addLong(22)
            }
            rc.field("int_test2", 1) {
              rc.addLong(300)
            }
          }
        }, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addLong(-21)
            }
            rc.field("int_test2", 1) {
              rc.addLong(-301)
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(22, 300),
          Row(-21, -301)), data)
      }
    })
  }

  test("int64 - unsigned") {
    withGpuSparkSession(spark => {
      // TODO the UINT is not supported on older version of spark. Need to figure out where it is
      //  supported so the test works properly
      val schema =
      """message spark {
        |  required int64 uint_test (UINT_64);
        |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/UINT64_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("uint_test", 0) {
              rc.addLong(22)
            }
          }
        }, { rc =>
          rc.message {
            rc.field("uint_test", 0) {
              rc.addLong(21)
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(22), Row(21)), data)
      }
    })
  }

  test("int32 Decimal") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required int32 int_test (DECIMAL(9,2));
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/DEC32_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addInteger(1)
            }
          }
        }, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addInteger(2)
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(BigDecimal("0.01")), Row(BigDecimal("0.02"))), data)
      }
    })
  }

  test("int64 Decimal") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required int64 int_test (DECIMAL(9,2));
          |  required int64 long_test (DECIMAL(15,0));
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/DEC64_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addLong(1)
            }
            rc.field("long_test", 1) {
              rc.addLong(100)
            }
          }
        }, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addLong(2)
            }
            rc.field("long_test", 1) {
              rc.addLong(201)
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("100")),
          Row(BigDecimal("0.02"), BigDecimal("201"))), data)
      }
    })
  }

  //https://github.com/NVIDIA/spark-rapids/issues/6915
  test("FIXED_LEN_BYTE_ARRAY(16) Decimal") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required fixed_len_byte_array(16) int_test (DECIMAL(9,2));
          |  required fixed_len_byte_array(16) long_test (DECIMAL(15,0));
          |  required fixed_len_byte_array(16) bin_test (DECIMAL(38,4));
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/FIXED_BIN16_DEC_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
            }
            rc.field("long_test", 1) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
            }
            rc.field("bin_test", 2) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
            }
          }
        }, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
            }
            rc.field("long_test", 1) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
            }

            rc.field("bin_test", 2) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("1"), BigDecimal("0.0001")),
          Row(BigDecimal("0.02"), BigDecimal("2"), BigDecimal("0.0002"))), data)
      }
    })
  }

  test("FIXED_LEN_BYTE_ARRAY(15) Decimal") {
    withGpuSparkSession(spark => {
      val schema =
        """message spark {
          |  required fixed_len_byte_array(15) int_test (DECIMAL(9,2));
          |  required fixed_len_byte_array(15) long_test (DECIMAL(15,0));
          |  required fixed_len_byte_array(15) bin_test (DECIMAL(35,4));
          |}
        """.stripMargin

      withTempDir(spark) { dir =>
        val testPath = dir + "/FIXED_BIN15_DEC_TEST.parquet"
        writeDirect(testPath, schema, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
            }
            rc.field("long_test", 1) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
            }
            rc.field("bin_test", 2) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
            }
          }
        }, { rc =>
          rc.message {
            rc.field("int_test", 0) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
            }
            rc.field("long_test", 1) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
            }

            rc.field("bin_test", 2) {
              rc.addBinary(Binary.fromConstantByteArray(
                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
            }
          }
        })

        val data = spark.read.parquet(testPath).collect()
        sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("1"), BigDecimal("0.0001")),
          Row(BigDecimal("0.02"), BigDecimal("2"), BigDecimal("0.0002"))), data)
      }
    })
  }

//  //https://github.com/NVIDIA/spark-rapids/issues/6915
//  test("binary Decimal") {
//    withGpuSparkSession(spark => {
//      val schema =
//        """message spark {
//          |  required binary int_test (DECIMAL(9,2));
//          |  required binary long_test (DECIMAL(15,0));
//          |  required binary bin_test (DECIMAL(35,4));
//          |}
//        """.stripMargin
//
//      withTempDir(spark) { dir =>
//        val testPath = dir + "/BINARY_DEC_TEST.parquet"
//        writeDirect(testPath, schema, { rc =>
//          rc.message {
//            rc.field("int_test", 0) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x00, 0x00, 0x01)))
//            }
//            rc.field("long_test", 1) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
//            }
//            rc.field("bin_test", 2) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
//                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
//            }
//          }
//        }, { rc =>
//          rc.message {
//            rc.field("int_test", 0) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x02)))
//            }
//            rc.field("long_test", 1) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x02)))
//            }
//
//            rc.field("bin_test", 2) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)))
//            }
//          }
//        })
//
//        val data = spark.read.parquet(testPath).collect()
//        sameRows(Seq(Row(BigDecimal("0.01"), BigDecimal("1"), BigDecimal("0.0001")),
//          Row(BigDecimal("0.02"), BigDecimal("2"), BigDecimal("0.0002"))), data)
//      }
//    },
//      // To make the CPU happy we need to turn off the vectorized reader
//      new SparkConf().set("spark.sql.parquet.enableVectorizedReader", "false"))
//  }
//
//  //https://github.com/NVIDIA/spark-rapids/issues/6915
//  // This bypasses the rapids accelerator code and gets errors in CUDF itself.
//  test("binary Decimal2") {
//    withGpuSparkSession(spark => {
//      val schema =
//        """message spark {
//          |  required binary bin_test (DECIMAL(19,0));
//          |}
//        """.stripMargin
//
//      withTempDir(spark) { dir =>
//        val testPath = dir + "/BINARY_DEC2_TEST.parquet"
//        writeDirect(testPath, schema, { rc =>
//          rc.message {
//            rc.field("bin_test", 0) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)))
//            }
//          }
//        }, { rc =>
//          rc.message {
//            rc.field("bin_test", 0) {
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(0x02)))
//            }
//          }
//        }, { rc =>
//          rc.message {
//            rc.field("bin_test", 0) {
//              // This one will do an overflow in the size of the binary data,
//              // but the top bytes are zero so it is okay
//              rc.addBinary(Binary.fromConstantByteArray(
//                Array(
//                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
//                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03)))
//            }
//          }
//        })
//
//        val data = spark.read.parquet(testPath).collect()
//        sameRows(Seq(Row(BigDecimal("1")),
//          Row(BigDecimal("2")),
//          Row(BigDecimal("3"))), data)
//      }
//    },
//      // To make the CPU happy we need to turn off the vectorized reader
//      new SparkConf().set("spark.sql.parquet.enableVectorizedReader", "false"))
//  }

//  test("SINGLE_GROUP_ARRAY") {
//    withCpuSparkSession(spark => {
//      val avroStyleSchema =
//        """message avro_style {
//          |  required group f (LIST) {
//          |    repeated int32 array;
//          |  }
//          |}
//        """.stripMargin
//
//      withTempDir(spark) { dir =>
//        val testPath = dir + "/BOBBY_TEST.parquet"
//        writeDirect(testPath, avroStyleSchema, { rc =>
//          rc.message {
//            rc.field("f", 0) {
//              rc.group {
//                rc.field("array", 0) {
//                  rc.addInteger(0)
//                  rc.addInteger(1)
//                }
//              }
//            }
//          }
//        })
//
//        val data = spark.read.parquet(testPath).collect()
//        assert(data == Array(Row(Array(0, 1))))
//      }
//    })
//  }
}
