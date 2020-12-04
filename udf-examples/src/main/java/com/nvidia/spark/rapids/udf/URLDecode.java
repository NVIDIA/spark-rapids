/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.udf;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class URLDecode extends UDF implements RapidsUDF {

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

  @Override
  public ColumnVector evaluateColumnar(ColumnVector... args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Unexpected argument count: " + args.length);
    }
    ColumnVector input = args[0];
    if (!input.getType().equals(DType.STRING)) {
      throw new IllegalArgumentException("Argument type is not a string column: " +
          input.getType());
    }

    // The cudf urlDecode does not convert '+' to a space, so do that as a pre-pass first.
    try (Scalar plusScalar = Scalar.fromString("+");
         Scalar spaceScalar = Scalar.fromString(" ");
         ColumnVector replaced = input.stringReplace(plusScalar, spaceScalar)) {
      return replaced.urlDecode();
    }
  }
}
