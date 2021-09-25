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

package com.nvidia.spark.rapids.udf.java;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF1;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * A Java user-defined function (UDF) that URL-encodes strings.
 * This class demonstrates how to implement a Java UDF that also
 * provides a RAPIDS implementation that can run on the GPU when the query
 * is executed with the RAPIDS Accelerator for Apache Spark.
 */
public class URLEncode implements UDF1<String, String>, RapidsUDF {
  /** Row-by-row implementation that executes on the CPU */
  @Override
  public String call(String s) {
    if (s == null) {
      return null;
    }
    try {
      return URLEncoder.encode(s, "utf-8")
          .replace("+", "%20")
          .replace("*", "%2A")
          .replace("%7E", "~");
    } catch (UnsupportedEncodingException e) {
      // utf-8 is a builtin, standard encoding, so this should never happen
      throw new RuntimeException(e);
    }
  }

  /** Columnar implementation that runs on the GPU */
  @Override
  public ColumnVector evaluateColumnar(ColumnVector... args) {
    // The CPU implementation takes a single string argument, so similarly
    // there should only be one column argument of type STRING.
    if (args.length != 1) {
      throw new IllegalArgumentException("Unexpected argument count: " + args.length);
    }
    ColumnVector input = args[0];
    if (!input.getType().equals(DType.STRING)) {
      throw new IllegalArgumentException("Argument type is not a string column: " +
          input.getType());
    }

    return input.urlEncode();
  }
}
