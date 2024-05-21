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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import java.io.File
import java.util.TimeZone

import com.nvidia.spark.rapids.{GpuProjectExec, TestStats}
import org.apache.commons.io.{FileUtils => fu}
import org.apache.commons.math3.util.Precision
import org.scalactic.TripleEqualsSupport.Spread
import scala.collection.mutable

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsQueryTestUtil.isNaNOrInf
import org.apache.spark.sql.types._


trait RapidsTestsTrait extends RapidsTestsCommonTrait {

  val originalTimeZone = TimeZone.getDefault

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      fu.forceDelete(basePathDir)
    }
    fu.forceMkdir(basePathDir)
    fu.forceMkdir(new File(warehouse))
    fu.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
    initializeSession()
    _spark.sparkContext.setLogLevel("WARN")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(originalTimeZone)

    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
    logInfo(
      "Test suite: " + this.getClass.getSimpleName +
        "; Suite test number: " + TestStats.suiteTestNumber +
        "; OffloadRapids number: " + TestStats.offloadRapidsTestNumber + "\n")
    TestStats.printMarkdown(this.getClass.getSimpleName)
    TestStats.reset()
  }

  protected def initializeSession(): Unit = {
    if (_spark == null) {
      val sparkBuilder = SparkSession
        .builder()
        .master(s"local[2]")
        // Avoid static evaluation for literal input by spark catalyst.
        .config(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
          ConvertToLocalRelation.ruleName +
            "," + ConstantFolding.ruleName)
        .config("spark.rapids.sql.enabled", "true")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.sql.queryExecutionListeners",
          "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.rapids.sql.explain", "ALL")
        .config("spark.rapids.sql.test.isFoldableNonLitAllowed", "true")
        // uncomment below config to run `strict mode`, where fallback to CPU is treated as fail
        // .config("spark.rapids.sql.test.enabled", "true")
        // .config("spark.rapids.sql.test.allowedNonGpu",
        // "SerializeFromObjectExec,DeserializeToObjectExec,ExternalRDDScanExec")
        .appName("rapids spark plugin running Vanilla Spark UT")

      _spark = sparkBuilder
        .config("spark.unsafe.exceptionOnMemoryLeak", "true")
        .getOrCreate()
    }
  }

  protected var _spark: SparkSession = null

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)

    rapidsCheckExpression(expr, expected, inputRow)
  }

  /**
   * Sort map data by key and return the sorted key array and value array.
   *
   * @param input
   * : input map data.
   * @param kt
   * : key type.
   * @param vt
   * : value type.
   * @return
   * the sorted key array and value array.
   */
  private def getSortedArrays(
      input: MapData,
      kt: DataType,
      vt: DataType): (ArrayData, ArrayData) = {
    val keyArray = input.keyArray().toArray[Any](kt)
    val valueArray = input.valueArray().toArray[Any](vt)
    val newMap = (keyArray.zip(valueArray)).toMap
    val sortedMap = mutable.SortedMap(newMap.toSeq: _*)(TypeUtils.getInterpretedOrdering(kt))
    (new GenericArrayData(sortedMap.keys.toArray), new GenericArrayData(sortedMap.values.toArray))
  }

  override protected def checkResult(
      result: Any,
      expected: Any,
      exprDataType: DataType,
      exprNullable: Boolean): Boolean = {
    val dataType = UserDefinedType.sqlType(exprDataType)

    // The result is null for a non-nullable expression
    assert(result != null || exprNullable, "exprNullable should be true if result is null")
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double @unchecked]) =>
        expected.asInstanceOf[Spread[Double]].isWithin(result)
      case (result: InternalRow, expected: InternalRow) =>
        val st = dataType.asInstanceOf[StructType]
        assert(result.numFields == st.length && expected.numFields == st.length)
        st.zipWithIndex.forall {
          case (f, i) =>
            checkResult(
              result.get(i, f.dataType),
              expected.get(i, f.dataType),
              f.dataType,
              f.nullable)
        }
      case (result: ArrayData, expected: ArrayData) =>
        result.numElements == expected.numElements && {
          val ArrayType(et, cn) = dataType.asInstanceOf[ArrayType]
          var isSame = true
          var i = 0
          while (isSame && i < result.numElements) {
            isSame = checkResult(result.get(i, et), expected.get(i, et), et, cn)
            i += 1
          }
          isSame
        }
      case (result: MapData, expected: MapData) =>
        val MapType(kt, vt, vcn) = dataType.asInstanceOf[MapType]
        checkResult(
          getSortedArrays(result, kt, vt)._1,
          getSortedArrays(expected, kt, vt)._1,
          ArrayType(kt, containsNull = false),
          exprNullable = false) && checkResult(
          getSortedArrays(result, kt, vt)._2,
          getSortedArrays(expected, kt, vt)._2,
          ArrayType(vt, vcn),
          exprNullable = false)
      case (result: Double, expected: Double) =>
        if (
          (isNaNOrInf(result) || isNaNOrInf(expected))
            || (result == -0.0) || (expected == -0.0)
        ) {
          java.lang.Double.doubleToRawLongBits(result) ==
            java.lang.Double.doubleToRawLongBits(expected)
        } else {
          Precision.equalsWithRelativeTolerance(result, expected, 0.00001d)
        }
      case (result: Float, expected: Float) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Row, expected: InternalRow) => result.toSeq == expected.toSeq(result.schema)
      case _ =>
        result == expected
    }
  }

  def checkDataTypeSupported(expr: Expression): Boolean = {
    RapidsTestConstants.SUPPORTED_DATA_TYPE.acceptsType(expr.dataType)
  }

  def rapidsCheckExpression(origExpr: Expression, expected: Any, inputRow: InternalRow): Unit = {
    // many of of the expressions in RAPIDS do not support
    // vectorized parameters. (e.g. regexp_replace)
    // So we downgrade all expression
    // evaluation to use scalar parameters.
    // In a follow-up issue we'll take care of the expressions
    // those already support vectorized paramters.
    val expression = origExpr.transformUp {
      case BoundReference(ordinal, dataType, _) =>
        Literal(inputRow.asInstanceOf[GenericInternalRow].get(ordinal, dataType), dataType)
    }
    val resultDF = _spark.range(0, 1).select(Column(expression))
    val result = resultDF.collect()
    TestStats.testUnitNumber = TestStats.testUnitNumber + 1
    if (
      checkDataTypeSupported(expression) &&
        expression.children.forall(checkDataTypeSupported)
    ) {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: GpuProjectExec => p
      }
      if (projectTransformer.size == 1) {
        TestStats.offloadRapidsUnitNumber += 1
        logInfo("Offload to native backend in the test.\n")
      } else {
        logInfo("Not supported in native backend, fall back to vanilla spark in the test.\n")
        shouldNotFallback()
      }
    } else {
      logInfo("Has unsupported data type, fall back to vanilla spark.\n")
      shouldNotFallback()
    }

    if (
      !(checkResult(result.head.get(0), expected, expression.dataType, expression.nullable)
        || checkResult(
        CatalystTypeConverters.createToCatalystConverter(expression.dataType)(
          result.head.get(0)
        ), // decimal precision is wrong from value
        CatalystTypeConverters.convertToCatalyst(expected),
        expression.dataType,
        expression.nullable
      ))
    ) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(
        s"Incorrect evaluation: $expression, " +
          s"actual: ${result.head.get(0)}, " +
          s"expected: $expected$input")
    }
  }

  def shouldNotFallback(): Unit = {
    TestStats.offloadRapids = false
  }

  def canConvertToDataFrame(inputRow: InternalRow): Boolean = {
    if (inputRow == EmptyRow || inputRow == InternalRow.empty) {
      return true
    }
    if (!inputRow.isInstanceOf[GenericInternalRow]) {
      return false
    }
    val values = inputRow.asInstanceOf[GenericInternalRow].values
    for (value <- values) {
      value match {
        case _: MapData => return false
        case _: ArrayData => return false
        case _: InternalRow => return false
        case _ =>
      }
    }
    true
  }
}
