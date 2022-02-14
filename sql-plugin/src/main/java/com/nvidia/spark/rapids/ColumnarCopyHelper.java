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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.HostColumnVector.ColumnBuilder;

import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A helper class which efficiently transfers different types of host columnar data into cuDF.
 */
public class ColumnarCopyHelper {

  public static void nullCopy(ColumnBuilder b, int rows) {
    for (int i = 0; i < rows; i++) {
      b.appendNull();
    }
  }

  public static void booleanCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getBoolean(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getBoolean(i));
    }
  }

  public static void byteCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getByte(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getByte(i));
    }
  }

  public static void shortCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getShort(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getShort(i));
    }
  }

  public static void intCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getInt(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getInt(i));
    }
  }

  public static void longCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getLong(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getLong(i));
    }
  }

  public static void floatCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getFloat(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getFloat(i));
    }
  }

  public static void doubleCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.append(cv.getDouble(i));
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.append(cv.getDouble(i));
    }
  }

  public static void stringCopy(ColumnVector cv, ColumnBuilder b, int rows) {
    if (!cv.hasNull()) {
      for (int i = 0; i < rows; i++) b.appendUTF8String(cv.getUTF8String(i).getBytes());
      return;
    }
    for (int i = 0; i < rows; i++) {
      if (cv.isNullAt(i)) {
        b.appendNull();
        continue;
      }
      b.appendUTF8String(cv.getUTF8String(i).getBytes());
    }
  }

  public static void decimal32Copy(ColumnVector cv, ColumnBuilder b, int rows,
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
        continue;
      }
      b.append((int) cv.getDecimal(i, precision, scale).toUnscaledLong());
    }
  }

  public static void decimal64Copy(ColumnVector cv, ColumnBuilder b, int rows,
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
        continue;
      }
      b.append(cv.getDecimal(i, precision, scale).toUnscaledLong());
    }
  }

  public static void decimal128Copy(ColumnVector cv, ColumnBuilder b, int rows,
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
        continue;
      }
      b.append(cv.getDecimal(i, precision, scale).toJavaBigDecimal());
    }
  }
}
