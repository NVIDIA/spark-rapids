/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * A Hive user-defined function (UDF) that decodes URL-encoded strings.
 * This class demonstrates how to implement a simple Hive UDF that also
 * provides a RAPIDS implementation that can run on the GPU when the query
 * is executed with the RAPIDS Accelerator for Apache Spark.
 */
public class URLDecode extends UDF implements RapidsUDF {

  /** Row-by-row implementation that executes on the CPU */
  public String evaluate(String s) {
    String result = null;
    if (s != null) {
      try {
        result = URLDecoder.decode(s, "utf-8");
      } catch (IllegalArgumentException ignored) {
        result = s;
      } catch (UnsupportedEncodingException e) {
        // utf-8 is a builtin, standard encoding, so this should never happen
        throw new RuntimeException(e);
      }
    }
    return result;
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

    // The cudf urlDecode does not convert '+' to a space, so do that as a pre-pass first.
    // All intermediate results are closed to avoid leaking GPU resources.
    try (Scalar plusScalar = Scalar.fromString("+");
         Scalar spaceScalar = Scalar.fromString(" ");
         ColumnVector replaced = input.stringReplace(plusScalar, spaceScalar)) {
      return replaced.urlDecode();
    }
  }
}
