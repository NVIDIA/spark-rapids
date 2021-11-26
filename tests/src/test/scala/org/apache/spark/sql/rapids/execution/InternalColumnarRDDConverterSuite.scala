package org.apache.spark.sql.rapids.execution

import org.apache.spark.sql.types.{BinaryType, DoubleType, StructField, StructType}

import com.nvidia.spark.rapids.{ColumnarToRowIterator, GpuBatchUtilsSuite, NoopMetric, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

class InternalColumnarRDDConverterSuite extends SparkQueryCompareTestSuite {
  test("transform binary data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("Binary", BinaryType),
      StructField("BinaryNotNull", BinaryType, nullable = false)))
    val numRows = 300
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getSeq(0) sameElements output.getBinary(0))
          }
          assert(input.getSeq(1) sameElements output.getBinary(1))
        }
      }
    }
  }

  test("transform Double data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("Double", DoubleType),
      StructField("DoubleNotNull", DoubleType, nullable = false)))
    val numRows = 300
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getSeq(0) sameElements output.getBinary(0))
          }
          assert(input.getSeq(1) sameElements output.getBinary(1))
        }
      }
    }
  }

}

