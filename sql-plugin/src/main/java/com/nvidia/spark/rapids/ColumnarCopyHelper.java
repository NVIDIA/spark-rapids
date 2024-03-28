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

package com.nvidia.spark.rapids;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A helper class which efficiently transfers different types of host columnar data into cuDF.
 * It is written in Java for two reasons:
 * 1. Scala for-loop is slower (Scala while-loop is identical to Java loop)
 * 2. Both ColumnBuilder and ColumnVector are Java classes
 */
public class ColumnarCopyHelper {

  public static void nullCopy(RapidsHostColumnBuilder b, int rows) {
    for (int i = 0; i < rows; i++) {
      b.appendNull();
    }
  }

  public static void booleanCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getBoolean(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getBoolean(i));
      }
    }
  }

  public static void byteCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getByte(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getByte(i));
      }
    }
  }

  public static void shortCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getShort(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getShort(i));
      }
    }
  }

  public static void intCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getInt(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getInt(i));
      }
    }
  }

  public static void longCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getLong(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getLong(i));
      }
    }
  }

  public static void floatCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getFloat(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getFloat(i));
      }
    }
  }

  public static void doubleCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getDouble(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getDouble(i));
      }
    }
  }

  public static void stringCopy(ColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.appendUTF8String(cv.getUTF8String(i).getBytes());
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.appendUTF8String(cv.getUTF8String(i).getBytes());
      }
    }
  }

  public static void decimal32Copy(WritableColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    intCopy(cv, b, rows);
  }

  public static void decimal64Copy(WritableColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    longCopy(cv, b, rows);
  }

  public static void decimal128Copy(WritableColumnVector cv, RapidsHostColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.appendDecimal128(cv.getBinary(i));
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.appendDecimal128(cv.getBinary(i));
      }
    }
  }

  public static void decimal32Copy(ColumnVector cv, RapidsHostColumnBuilder b, int rows,
      int precision, int scale) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append((int) cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append((int) cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
    }
  }

  public static void decimal64Copy(ColumnVector cv, RapidsHostColumnBuilder b, int rows,
      int precision, int scale) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getDecimal(i, precision, scale).toUnscaledLong());
      }
    }
  }

  public static void decimal128Copy(ColumnVector cv, RapidsHostColumnBuilder b, int rows,
      int precision, int scale) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) {
        b.append(cv.getDecimal(i, precision, scale).toJavaBigDecimal());
      }
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
      } else {
        b.append(cv.getDecimal(i, precision, scale).toJavaBigDecimal());
      }
    }
  }
}
