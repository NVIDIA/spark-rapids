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
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.mutable.WrappedArray;

/**
 * A Spark Java UDF that computes the cosine similarity between two float vectors.
 * The input vectors must have matching shapes, i.e.: same number of elements.
 * A null vector is supported, but null entries within the vector are not supported.
 */
public class CosineSimilarity
    implements UDF2<WrappedArray<Float>, WrappedArray<Float>, Float>, RapidsUDF {

  /** Row-by-row implementation that executes on the CPU */
  @Override
  public Float call(WrappedArray<Float> v1, WrappedArray<Float> v2) {
    if (v1 == null || v2 == null) {
      return null;
    }
    if (v1.length() != v2.length()) {
      throw new IllegalArgumentException("Array lengths must match: " +
          v1.length() + " != " + v2.length());
    }

    double dotProduct = 0;
    for (int i = 0; i < v1.length(); i++) {
      float f1 = v1.apply(i);
      float f2 = v2.apply(i);
      dotProduct += f1 * f2;
    }
    double magProduct = magnitude(v1) * magnitude(v2);
    return (float) (dotProduct / magProduct);
  }

  private double magnitude(WrappedArray<Float> v) {
    double sum = 0;
    for (int i = 0; i < v.length(); i++) {
      float x = v.apply(i);
      sum += x * x;
    }
    return Math.sqrt(sum);
  }

  /** Columnar implementation that processes data on the GPU */
  @Override
  public ColumnVector evaluateColumnar(ColumnVector... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("Unexpected argument count: " + args.length);
    }

    // Load the native code if it has not been already loaded. This is done here
    // rather than in a static code block since the driver may not have the
    // required CUDA environment.
    NativeUDFExamplesLoader.ensureLoaded();

    return new ColumnVector(cosineSimilarity(args[0].getNativeView(), args[1].getNativeView()));
  }

  /** Native implementation that computes on the GPU */
  private static native long cosineSimilarity(long vectorView1, long vectorView2);
}
