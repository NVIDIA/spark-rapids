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
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.math.BigDecimal;


/**
 * A simple HiveGenericUDF demo for DecimalType, which extracts and returns
 * the fraction part of the input Decimal data. So, the output data has the
 * same precision and scale as the input one.
 */
public class DecimalFraction extends GenericUDF implements RapidsUDF {
  private transient PrimitiveObjectInspector inputOI;

  @Override
  public String getDisplayString(String[] strings) {
    return getStandardDisplayString("DecimalFraction", strings);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("One argument is supported, found: " + arguments.length);
    }
    if (!(arguments[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("Unsupported argument type: " + arguments[0].getTypeName());
    }

    inputOI = (PrimitiveObjectInspector) arguments[0];
    if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
      throw new UDFArgumentException("Unsupported primitive type: " + inputOI.getPrimitiveCategory());
    }

    DecimalTypeInfo inputTypeInfo = (DecimalTypeInfo) inputOI.getTypeInfo();

    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputTypeInfo);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null || arguments[0].get() == null) {
      return null;
    }

    Object input = arguments[0].get();
    HiveDecimalWritable decimalWritable = (HiveDecimalWritable) inputOI.getPrimitiveWritableObject(input);
    BigDecimal decimalInput = decimalWritable.getHiveDecimal().bigDecimalValue();
    BigDecimal decimalResult = decimalInput.subtract(new BigDecimal(decimalInput.toBigInteger()));
    HiveDecimalWritable result = new HiveDecimalWritable(decimalWritable);
    result.set(HiveDecimal.create(decimalResult));

    return result;
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
