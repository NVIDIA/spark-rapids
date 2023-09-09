/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, RegexProgram, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.GpuCastShims

import org.apache.spark.sql.types._

trait ToStringBase {
  // The brackets that are used in casting structs and maps to strings
  protected def leftBracket: String

  protected def rightBracket: String

  // The string value to use to represent null elements in array/struct/map.
  protected def nullString: String

//  protected def useDecimalPlainString: Boolean
//
//  protected def useHexFormatForBinary: Boolean

  def castToString(
      input: ColumnView,
      fromDataType: DataType,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = fromDataType match {
    case StringType => input.copyToColumnVector()
    case DateType => input.asStrings("%Y-%m-%d")
    case TimestampType => castTimestampToString(input)
    case FloatType | DoubleType => castFloatingTypeToString(input)
    case BinaryType => castBinToString(input)
    case _: DecimalType => GpuCastShims.CastDecimalToString(input, ansiMode)
    case StructType(fields) =>
      castStructToString(input, fields, ansiMode, legacyCastToString,
        stringToDateAnsiModeEnabled)

    case ArrayType(elementType, _) =>
      castArrayToString(
        input, elementType, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled
      )
    case from: MapType =>
      castMapToString(input, from, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)
  }

  private def castTimestampToString(input: ColumnView): ColumnVector = {
    // the complexity in this function is due to Spark's rules for truncating
    // the fractional part of the timestamp string. Any trailing decimal place
    // or zeroes should be truncated
    // ".000000" -> ""
    // ".000100" -> ".0001"
    // ".100000" -> ".1"
    // ".101010" -> ".10101"
    withResource(input.castTo(DType.TIMESTAMP_MICROSECONDS)) { micros =>
      withResource(micros.asStrings("%Y-%m-%d %H:%M:%S.%6f")) { cv =>
        // to keep code complexity down, do a first pass that
        // removes ".000000" using simple string replace
        val firstPass = withResource(Scalar.fromString(".000000")) { search =>
          withResource(Scalar.fromString("")) { replace =>
            cv.stringReplace(search, replace)
          }
        }
        // now remove trailing zeroes from any remaining fractional parts
        // the first group captures everything between
        // the decimal point and the last non-zero digit
        // the second group (non-capture) covers the remaining zeroes
        withResource(firstPass) { _ =>
          val prog = new RegexProgram("(\\.[0-9]*[1-9]+)(?:0+)?$")
          firstPass.stringReplaceWithBackrefs(prog, "\\1")
        }
      }
    }
  }

  private[rapids] def castFloatingTypeToString(input: ColumnView): ColumnVector = {
    withResource(input.castTo(DType.STRING)) { cudfCast =>

      // replace "e+" with "E"
      val replaceExponent = withResource(Scalar.fromString("e+")) { cudfExponent =>
        withResource(Scalar.fromString("E")) { sparkExponent =>
          cudfCast.stringReplace(cudfExponent, sparkExponent)
        }
      }

      // replace "Inf" with "Infinity"
      withResource(replaceExponent) { replaceExponent =>
        withResource(Scalar.fromString("Inf")) { cudfInf =>
          withResource(Scalar.fromString("Infinity")) { sparkInfinity =>
            replaceExponent.stringReplace(cudfInf, sparkInfinity)
          }
        }
      }
    }
  }

  private def castBinToString(input: ColumnView): ColumnVector = {
    // Spark interprets the binary as UTF-8 bytes. So the layout of the
    // binary and the layout of the string are the same. We just need to play some games with
    // the CPU side metadata to make CUDF think it is a String.
    // Sadly there is no simple CUDF API to do this, so for now we pull it apart and put
    // it back together again
    withResource(input.getChildColumnView(0)) { dataCol =>
      withResource(new ColumnView(DType.STRING, input.getRowCount,
        Optional.of[java.lang.Long](input.getNullCount),
        dataCol.getData, input.getValid, input.getOffsets)) { cv =>
        cv.copyToColumnVector()
      }
    }
  }

  /**
   * A 5 steps solution for concatenating string array column. <p>
   * Giving an input with 3 rows:
   * `[ ["1", "2", null, "3"], [], null]` <p>
   * When `legacyCastToString = true`: <p>
   * Step 1: add space char in the front of all not-null elements:
   * `[ [" 1", " 2", null, " 3"], [], null]` <p>
   * step 2: cast `null` elements to their string representation :
   * `[ [" 1", " 2", "", " 3"], [], null]`(here we use "" to represent null) <p>
   * step 3: concatenate list elements, seperated by `","`:
   * `[" 1, 2,, 3", null, null]` <p>
   * step 4: remove the first char, if it is an `' '`:
   * `["1, 2,, 3", null, null]` <p>
   * step 5: replace nulls with empty string:
   * `["1, 2,, 3", "", ""]` <p>
   *
   * when `legacyCastToString = false`, step 1, 4 are skipped
   */
  private def concatenateStringArrayElements(
      input: ColumnView,
      legacyCastToString: Boolean): ColumnVector = {
    val emptyStr = ""
    val spaceStr = " "
    val nullStr = if (legacyCastToString) "" else nullString
    val sepStr = if (legacyCastToString) "," else ", "
    withResource(
      Seq(emptyStr, spaceStr, nullStr, sepStr).safeMap(Scalar.fromString)
    ) { case Seq(empty, space, nullRep, sep) =>

      val withSpacesIfLegacy = if (!legacyCastToString) {
        withResource(input.getChildColumnView(0)) {
          _.replaceNulls(nullRep)
        }
      } else {
        // add a space string to each non-null element
        val (strChild, childNotNull, numElements) =
          withResource(input.getChildColumnView(0)) { childCol =>
            closeOnExcept(childCol.replaceNulls(nullRep)) {
              (_, childCol.isNotNull(), childCol.getRowCount.toInt)
            }
          }
        withResource(Seq(strChild, childNotNull)) { _ =>
          val hasSpaces = withResource(ColumnVector.fromScalar(space, numElements)) { spaceCol =>
            ColumnVector.stringConcatenate(Array(spaceCol, strChild))
          }
          withResource(hasSpaces) {
            childNotNull.ifElse(_, strChild)
          }
        }
      }
      val concatenated = withResource(withSpacesIfLegacy) { strChildCol =>
        withResource(input.replaceListChild(strChildCol)) { strArrayCol =>
          withResource(ColumnVector.fromScalar(sep, input.getRowCount.toInt)) {
            strArrayCol.stringConcatenateListElements
          }
        }
      }
      val strCol = withResource(concatenated) {
        _.replaceNulls(empty)
      }
      if (!legacyCastToString) {
        strCol
      } else {
        // If the first char of a string is ' ', remove it (only for legacyCastToString = true)
        withResource(strCol) { _ =>
          withResource(strCol.startsWith(space)) { startsWithSpace =>
            withResource(strCol.substring(1)) { remain =>
              startsWithSpace.ifElse(remain, strCol)
            }
          }
        }
      }
    }
  }

  private def castArrayToString(input: ColumnView,
      elementType: DataType,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    val (leftStr, rightStr) = ("[", "]")
    val emptyStr = ""
    val nullStr = nullString
    val numRows = input.getRowCount.toInt

    withResource(
      Seq(leftStr, rightStr, emptyStr, nullStr).safeMap(Scalar.fromString)
    ) { case Seq(left, right, empty, nullRep) =>
      val strChildContainsNull = withResource(input.getChildColumnView(0)) { child =>
        AnotherCastClass(child, elementType, StringType, ansiMode,
          legacyCastToString, stringToDateAnsiModeEnabled)
      }

      val concatenated = withResource(strChildContainsNull) { _ =>
        withResource(input.replaceListChild(strChildContainsNull)) {
          concatenateStringArrayElements(_, legacyCastToString)
        }
      }

      // Add brackets to each string. Ex: ["1, 2, 3", "4, 5"] => ["[1, 2, 3]", "[4, 5]"]
      val hasBrackets = withResource(concatenated) { _ =>
        withResource(
          Seq(left, right).safeMap(ColumnVector.fromScalar(_, numRows))
        ) { case Seq(leftColumn, rightColumn) =>
          ColumnVector.stringConcatenate(empty, nullRep, Array(leftColumn, concatenated,
            rightColumn))
        }
      }
      withResource(hasBrackets) {
        _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
      }
    }
  }

  private def castMapToString(
      input: ColumnView,
      from: MapType,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    val numRows = input.getRowCount.toInt
    val (arrowStr, emptyStr, spaceStr) = ("->", "", " ")
    val (leftStr, rightStr, nullStr) = (leftBracket, rightBracket, nullString)

    // cast the key column and value column to string columns
    val (strKey, strValue) = withResource(input.getChildColumnView(0)) { kvStructColumn =>
      val strKey = withResource(kvStructColumn.getChildColumnView(0)) { keyColumn =>
        AnotherCastClass(
          keyColumn, from.keyType, StringType, ansiMode, legacyCastToString,
          stringToDateAnsiModeEnabled)
      }
      val strValue = closeOnExcept(strKey) { _ =>
        withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
          AnotherCastClass(
            valueColumn, from.valueType, StringType, ansiMode, legacyCastToString,
            stringToDateAnsiModeEnabled)
        }
      }
      (strKey, strValue)
    }

    // concatenate the key-value pairs to string
    // Example: ("key", "value") -> "key -> value"
    withResource(
      Seq(leftStr, rightStr, arrowStr, emptyStr, nullStr, spaceStr).safeMap(Scalar.fromString)
    ) { case Seq(leftScalar, rightScalar, arrowScalar, emptyScalar, nullScalar, spaceScalar) =>
      val strElements = withResource(Seq(strKey, strValue)) { case Seq(strKey, strValue) =>
        val numElements = strKey.getRowCount.toInt
        withResource(Seq(spaceScalar, arrowScalar).safeMap(ColumnVector.fromScalar(_, numElements))
        ) { case Seq(spaceCol, arrowCol) =>
          if (legacyCastToString) {
            withResource(
              spaceCol.mergeAndSetValidity(BinaryOp.BITWISE_AND, strValue)
            ) { spaceBetweenSepAndVal =>
              ColumnVector.stringConcatenate(
                emptyScalar, nullScalar,
                Array(strKey, spaceCol, arrowCol, spaceBetweenSepAndVal, strValue))
            }
          } else {
            ColumnVector.stringConcatenate(
              emptyScalar, nullScalar, Array(strKey, spaceCol, arrowCol, spaceCol, strValue))
          }
        }
      }

      // concatenate elements
      val strCol = withResource(strElements) { _ =>
        withResource(input.replaceListChild(strElements)) {
          concatenateStringArrayElements(_, legacyCastToString)
        }
      }
      val resPreValidityFix = withResource(strCol) { _ =>
        withResource(
          Seq(leftScalar, rightScalar).safeMap(ColumnVector.fromScalar(_, numRows))
        ) { case Seq(leftCol, rightCol) =>
          ColumnVector.stringConcatenate(
            emptyScalar, nullScalar, Array(leftCol, strCol, rightCol))
        }
      }
      withResource(resPreValidityFix) {
        _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
      }
    }
  }

  private def castStructToString(
      input: ColumnView,
      inputSchema: Array[StructField],
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    val (leftStr, rightStr) = (leftBracket, rightBracket)
    val emptyStr = ""
    val nullStr = nullString
    val separatorStr = if (legacyCastToString) "," else ", "
    val spaceStr = " "
    val numRows = input.getRowCount.toInt
    val numInputColumns = input.getNumChildren

    def doCastStructToString(
        emptyScalar: Scalar,
        nullScalar: Scalar,
        sepColumn: ColumnVector,
        spaceColumn: ColumnVector,
        leftColumn: ColumnVector,
        rightColumn: ColumnVector): ColumnVector = {
      withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
        // legacy: [firstCol
        //   3.1+: {firstCol
        columns += leftColumn.incRefCount()
        withResource(input.getChildColumnView(0)) { firstColumnView =>
          columns += AnotherCastClass(firstColumnView, inputSchema.head.dataType, StringType,
            ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)
        }
        for (nonFirstIndex <- 1 until numInputColumns) {
          withResource(input.getChildColumnView(nonFirstIndex)) { nonFirstColumnView =>
            // legacy: ","
            //   3.1+: ", "
            columns += sepColumn.incRefCount()
            val nonFirstColumn = AnotherCastClass(nonFirstColumnView,
              inputSchema(nonFirstIndex).dataType, StringType, ansiMode,
              legacyCastToString, stringToDateAnsiModeEnabled)
            if (legacyCastToString) {
              // " " if non-null
              columns += spaceColumn.mergeAndSetValidity(BinaryOp.BITWISE_AND, nonFirstColumnView)
            }
            columns += nonFirstColumn
          }
        }

        columns += rightColumn.incRefCount()
        withResource(ColumnVector.stringConcatenate(emptyScalar, nullScalar, columns.toArray))(
          _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input) // original whole row is null
        )
      }
    }

    withResource(Seq(emptyStr, nullStr, separatorStr, spaceStr, leftStr, rightStr)
      .safeMap(Scalar.fromString)) {
      case Seq(emptyScalar, nullScalar, columnScalars@_*) =>

        withResource(
          columnScalars.safeMap(s => ColumnVector.fromScalar(s, numRows))
        ) { case Seq(sepColumn, spaceColumn, leftColumn, rightColumn) =>

          doCastStructToString(emptyScalar, nullScalar, sepColumn,
            spaceColumn, leftColumn, rightColumn)
        }
    }
  }

}
