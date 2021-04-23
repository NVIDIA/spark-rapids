/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.udf.hive;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.NativeDepsLoader;
import com.nvidia.spark.RapidsUDF;
import com.nvidia.spark.rapids.udf.java.NativeUDFExamplesLoader;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * A user-defined function (UDF) that counts the words in a string.
 * This avoids the manifestation of intermediate results required when
 * splitting the string on whitespace and counting the split results.
 * <p>
 * This class demonstrates how to implement a Hive UDF with a RAPIDS
 * implementation that uses custom native code.
 */
public class StringWordCount extends UDF implements RapidsUDF {
  private volatile boolean isNativeCodeLoaded = false;

  /** Row-by-row implementation that executes on the CPU */
  public Integer evaluate(String str) {
    if (str == null) {
      return null;
    }

    int numWords = 0;
    // run of whitespace is considered a single delimiter
    boolean spaces = true;
    for (int idx = 0; idx < str.length(); idx++) {
      char ch = str.charAt(idx);
      if (spaces != (ch <= ' ')) {
        if (spaces) {
          numWords++;
        }
        spaces = !spaces;
      }
    }
    return numWords;
  }

  /** Columnar implementation that runs on the GPU */
  @Override
  public ColumnVector evaluateColumnar(ColumnVector... args) {
    // The CPU implementation takes a single string argument, so similarly
    // there should only be one column argument of type STRING.
    if (args.length != 1) {
      throw new IllegalArgumentException("Unexpected argument count: " + args.length);
    }
    ColumnVector strs = args[0];
    if (!strs.getType().equals(DType.STRING)) {
      throw new IllegalArgumentException("type mismatch, expected strings but found " +
          strs.getType());
    }

    // Load the native code if it has not been already loaded. This is done here
    // rather than in a static code block since the driver may not have the
    // required CUDA environment.
    NativeUDFExamplesLoader.ensureLoaded();

    return new ColumnVector(countWords(strs.getNativeView()));
  }

  private static native long countWords(long stringsView);
}
