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
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * A Hive user-defined function (UDF) that URL-encodes strings.
 * This class demonstrates how to implement a Hive GenericUDF that also
 * provides a RAPIDS implementation that can run on the GPU when the query
 * is executed with the RAPIDS Accelerator for Apache Spark.
 */
public class URLEncode extends GenericUDF implements RapidsUDF {
  private transient PrimitiveObjectInspectorConverter.TextConverter converter;
  private final Text textResult = new Text();

  /** Standard getDisplayString method for implementing GenericUDF */
  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("urlencode", children);
  }

  /** Standard initialize method for implementing GenericUDF for a single string parameter */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("One argument is supported, found: " + arguments.length);
    }
    if (!(arguments[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("Unsupported argument type: " + arguments[0].getTypeName());
    }
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) arguments[0];
    switch (poi.getPrimitiveCategory()) {
      case STRING:
      case CHAR:
      case VARCHAR:
        break;
      default:
        throw new UDFArgumentException("Unsupported primitive type: " + poi.getPrimitiveCategory());
    }

    converter = new PrimitiveObjectInspectorConverter.TextConverter(poi);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  /** Row-by-row implementation that executes on the CPU */
  @Override
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
    Text text = converter.convert(arguments[0].get());
    if (text == null) {
      return null;
    }
    String encoded;
    try {
      encoded = URLEncoder.encode(text.toString(), "utf-8")
          .replace("+", "%20")
          .replace("*", "%2A")
          .replace("%7E", "~");
    } catch (UnsupportedEncodingException e) {
      // utf-8 is a builtin, standard encoding, so this should never happen
      throw new RuntimeException(e);
    }
    textResult.set(encoded);
    return textResult;
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
