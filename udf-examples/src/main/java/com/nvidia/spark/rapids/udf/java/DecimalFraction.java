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
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF1;

import java.math.BigDecimal;

public class DecimalFraction implements UDF1<BigDecimal, BigDecimal>, RapidsUDF {

  @Override
  public BigDecimal call(BigDecimal dec) throws Exception {
    if (dec == null) {
      return null;
    }
    BigDecimal integral = new BigDecimal(dec.toBigInteger());
    return dec.subtract(integral);
  }

  @Override
  public ColumnVector evaluateColumnar(ColumnVector... args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Unexpected argument count: " + args.length);
    }
    ColumnVector input = args[0];
    if (!input.getType().isDecimalType()) {
      throw new IllegalArgumentException("Argument type is not a decimal column: " +
          input.getType());
    }

    try (Scalar nullScalar = Scalar.fromNull(input.getType());
         ColumnVector nullPredicate = input.isNull();
         ColumnVector integral = input.floor();
         ColumnVector fraction = input.sub(integral, input.getType())) {
      return nullPredicate.ifElse(nullScalar, fraction);
    }
  }
}
