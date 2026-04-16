/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.aggregate

import ai.rapids.cudf.{ColumnVector, DType, GroupByAggregation, HostColumnVector, Scalar, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuLiteral
import com.nvidia.spark.rapids.jni.BloomFilter
import com.nvidia.spark.rapids.shims.BloomFilterConstantsShims

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.internal.SQLConf.{
  RUNTIME_BLOOM_FILTER_MAX_NUM_BITS,
  RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.aggregate.GpuBloomFilterAggregate.optimalNumOfHashFunctions
import org.apache.spark.sql.types.{BinaryType, DataType}
case class GpuBloomFilterAggregate(
    child: Expression,
    estimatedNumItemsRequested: Long,
    numBitsRequested: Long,
    version: Int = BloomFilterConstantsShims.BLOOM_FILTER_FORMAT_VERSION,
    seed: Int = BloomFilter.DEFAULT_SEED) extends GpuAggregateFunction {

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bloom_filter_agg"

  private val estimatedNumItems: Long =
    GpuBloomFilterAggregate.clampEstimatedNumItems(estimatedNumItemsRequested)

  private val numBits: Long = GpuBloomFilterAggregate.clampNumBits(numBitsRequested)

  private lazy val numHashes: Int = optimalNumOfHashFunctions(estimatedNumItems, numBits)

  override def children: Seq[Expression] = Seq(child)

  override lazy val initialValues: Seq[Expression] = Seq(GpuLiteral(null, BinaryType))

  override val inputProjection: Seq[Expression] = Seq(child)

  override val updateAggregates: Seq[CudfAggregate] =
    Seq(GpuBloomFilterUpdate(numHashes, numBits, version, seed))

  override val mergeAggregates: Seq[CudfAggregate] = Seq(GpuBloomFilterMerge())

  private lazy val bloomAttr: AttributeReference = AttributeReference("bloomFilter", dataType)()

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(bloomAttr)

  override val evaluateExpression: Expression = bloomAttr
}

object GpuBloomFilterAggregate {
  def clampEstimatedNumItems(estimatedNumItemsRequested: Long): Long =
    Math.min(estimatedNumItemsRequested, SQLConf.get.getConf(RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS))

  def clampNumBits(numBitsRequested: Long): Long =
    Math.min(numBitsRequested, SQLConf.get.getConf(RUNTIME_BLOOM_FILTER_MAX_NUM_BITS))

  /**
   * From Spark's BloomFilter.optimalNumOfHashFunctions
   *
   * Computes the optimal k (number of hashes per item inserted in Bloom filter), given the
   * expected insertions and total number of bits in the Bloom filter.
   *
   * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param m total number of bits in Bloom filter (must be positive)
   */
  private def optimalNumOfHashFunctions(n: Long, m: Long): Int = {
    // (m / n) * log(2), but avoid truncation due to division!
    Math.max(1, Math.round(m.toDouble / n * Math.log(2)).toInt)
  }

  // Use the JNI BloomFilter API here instead of Spark's BloomFilter implementation because
  // some Databricks runtimes do not expose org.apache.spark.util.sketch.BloomFilter on the
  // compile/runtime classpath. This path still needs to produce Spark-compatible serialized
  // bloom filter bytes for the rare GPU-to-CPU aggregate buffer bridge case.
  def createEmptyBloomFilterBytes(
      effectiveEstimatedNumItems: Long,
      effectiveNumBits: Long,
      version: Int,
      seed: Int): Array[Byte] = {
    val numHashes = optimalNumOfHashFunctions(effectiveEstimatedNumItems, effectiveNumBits)
    withResource(BloomFilter.create(version, numHashes, effectiveNumBits, seed)) { bloomFilter =>
      withResource(bloomFilter.getListAsColumnView) { bloomFilterView =>
        withResource(bloomFilterView.copyToHost()) { hostBloomFilter =>
          val byteCount = Math.toIntExact(hostBloomFilter.getRowCount)
          val bytes = new Array[Byte](byteCount)
          if (byteCount > 0) {
            hostBloomFilter.getData.getBytes(bytes, 0, 0, byteCount)
          }
          bytes
        }
      }
    }
  }
}

case class GpuBloomFilterUpdate(
    numHashes: Int,
    numBits: Long,
    version: Int,
    seed: Int) extends CudfAggregate {
  override val reductionAggregate: ColumnVector => Scalar = (col: ColumnVector) => {
    closeOnExcept(BloomFilter.create(version, numHashes, numBits, seed)) { bloomFilter =>
      BloomFilter.put(bloomFilter, col)
      bloomFilter
    }
  }

  override lazy val groupByAggregate: GroupByAggregation =
    throw new UnsupportedOperationException("group by aggregations are not supported")

  override def dataType: DataType = BinaryType

  override val name: String = "gpu_bloom_filter_update"
}

case class GpuBloomFilterMerge() extends CudfAggregate {
  override val reductionAggregate: ColumnVector => Scalar = (col: ColumnVector) => {
    val nullCount = col.getNullCount
    if (nullCount == col.getRowCount) {
      // degenerate case, all columns are null
      Scalar.listFromNull(new HostColumnVector.BasicType(false, DType.UINT8))
    } else if (nullCount > 0) {
      // BloomFilter.merge does not handle nulls, so filter them out before merging
      withResource(col.isNotNull) { isNotNull =>
        withResource(new Table(col)) { table =>
          withResource(table.filter(isNotNull)) { filtered =>
            BloomFilter.merge(filtered.getColumn(0))
          }
        }
      }
    } else {
      BloomFilter.merge(col)
    }
  }

  override lazy val groupByAggregate: GroupByAggregation =
    throw new UnsupportedOperationException("group by aggregations are not supported")

  override def dataType: DataType = BinaryType

  override val name: String = "gpu_bloom_filter_merge"
}

case class CpuToGpuBloomFilterBufferConverter() extends CpuToGpuAggregateBufferConverter {
  override def createExpression(child: Expression): CpuToGpuBufferTransition =
    CpuToGpuBloomFilterBufferTransition(child)
}

case class CpuToGpuBloomFilterBufferTransition(override val child: Expression)
    extends CpuToGpuBufferTransition {
  override def dataType: DataType = BinaryType

  override protected def nullSafeEval(input: Any): Array[Byte] = input.asInstanceOf[Array[Byte]]
}

case class GpuToCpuBloomFilterBufferConverter(
    effectiveEstimatedNumItems: Long,
    effectiveNumBits: Long,
    version: Int = BloomFilterConstantsShims.BLOOM_FILTER_FORMAT_VERSION,
    seed: Int = BloomFilter.DEFAULT_SEED) extends GpuToCpuAggregateBufferConverter {
  override def createExpression(child: Expression): GpuToCpuBufferTransition =
    GpuToCpuBloomFilterBufferTransition(
      child, effectiveEstimatedNumItems, effectiveNumBits, version, seed)
}

case class GpuToCpuBloomFilterBufferTransition(
    override val child: Expression,
    effectiveEstimatedNumItems: Long,
    effectiveNumBits: Long,
    version: Int,
    seed: Int) extends GpuToCpuBufferTransition {

  private lazy val emptyBloomFilterBytes: Array[Byte] =
    GpuBloomFilterAggregate.createEmptyBloomFilterBytes(
      effectiveEstimatedNumItems, effectiveNumBits, version, seed)

  override def eval(input: InternalRow): Any = {
    val buffer = child.eval(input)
    if (buffer == null) {
      emptyBloomFilterBytes.clone()
    } else {
      buffer.asInstanceOf[Array[Byte]]
    }
  }

  // ShimUnaryExpression still requires nullSafeEval even though eval handles the null-to-empty
  // bloom filter rewrite directly for this bridge expression.
  override protected def nullSafeEval(input: Any): Array[Byte] = input.asInstanceOf[Array[Byte]]
}
