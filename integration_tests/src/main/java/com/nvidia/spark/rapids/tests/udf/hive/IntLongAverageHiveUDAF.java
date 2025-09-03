/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.udf.hive;

import java.util.ArrayList;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.GroupByAggregation;
import ai.rapids.cudf.GroupByAggregationOnColumn;
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.RapidsSimpleGroupByAggregation;
import com.nvidia.spark.RapidsUDAF;

import com.nvidia.spark.RapidsUDAFGroupByAggregation;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.LongType;

/** Used by hive_udaf_test */
@SuppressWarnings("deprecation")
public class IntLongAverageHiveUDAF extends AbstractGenericUDAFResolver implements RapidsUDAF {
  // ===== CPU Hive UDAF Implementation =====
  // Build an evaluator for the aggregation
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] args) throws SemanticException {
    if (args.length != 1) {
      throw new HadoopIllegalArgumentException("Exactly one argument is expected.");
    }
    PrimitiveObjectInspector.PrimitiveCategory inType =
        ((PrimitiveTypeInfo) args[0]).getPrimitiveCategory();
    if (inType == PrimitiveObjectInspector.PrimitiveCategory.LONG ||
        inType == PrimitiveObjectInspector.PrimitiveCategory.INT) {
      boolean isInt = inType == PrimitiveObjectInspector.PrimitiveCategory.INT;
      return new UDAFAverageEvaluatorLong(isInt);
    }
    throw new HadoopIllegalArgumentException("Only support 'long' or 'int' as input");
  }

  class AverageAggBuf extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    private long sum;
    private long count;
  }

  @SuppressWarnings("deprecation")
  class UDAFAverageEvaluatorLong extends GenericUDAFEvaluator {
    private final boolean isInt;

    UDAFAverageEvaluatorLong(boolean isInt) {
      this.isInt = isInt;
    }

    transient private PrimitiveObjectInspector inputOI;
    transient private StructObjectInspector tempOI;

    transient private StructField countField;
    transient private StructField sumField;

    transient private LongObjectInspector countFieldOI;
    transient private LongObjectInspector sumFieldOI;

    transient private Object[] partialRet;

    @Override
    public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);
      assert (parameters.length == 1);

      partialRet = new Object[2];
      partialRet[0] = new LongWritable(0);
      partialRet[1] = new LongWritable(0);
      // for the input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {
        tempOI = (StructObjectInspector) parameters[0];
        sumField = tempOI.getStructFieldRef("sum");
        countField = tempOI.getStructFieldRef("count");
        countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
        sumFieldOI = (LongObjectInspector) sumField.getFieldObjectInspector();
      }

      // for the output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a "long" count and a "long" sum.
        // a "long" count and a "long" sum.
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        ArrayList<String> fnames = new ArrayList<String>();
        fnames.add("sum");
        fnames.add("count");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fnames, foi);
      } else {
        if (isInt) {
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
          return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new AverageAggBuf();
    }

    @Override
    public void iterate(AggregationBuffer aggBuffer, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      Object obj = parameters[0];
      if (obj != null) {
        AverageAggBuf buf = (AverageAggBuf) aggBuffer;
        buf.count += 1;
        if (isInt) {
          buf.sum += PrimitiveObjectInspectorUtils.getInt(obj, inputOI);
        } else {
          buf.sum += PrimitiveObjectInspectorUtils.getLong(obj, inputOI);
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggBuffer) throws HiveException {
      AverageAggBuf buf = (AverageAggBuf) aggBuffer;
      ((LongWritable) partialRet[0]).set(buf.sum);
      ((LongWritable) partialRet[1]).set(buf.count);
      return partialRet;
    }

    @Override
    public void merge(AggregationBuffer aggBuffer, Object partial) throws HiveException {
      if (partial != null) {
        AverageAggBuf buf = (AverageAggBuf) aggBuffer;
        long count = countFieldOI.get(tempOI.getStructFieldData(partial, countField));
        long sum = sumFieldOI.get(tempOI.getStructFieldData(partial, sumField));
        buf.count += count;
        buf.sum += sum;
      }
    }

    @Override
    public Object terminate(AggregationBuffer aggBuffer) throws HiveException {
      AverageAggBuf buf = (AverageAggBuf) aggBuffer;
      if (buf.count == 0) {
        return null;
      } else {
        if (isInt) {
          IntWritable result = new IntWritable(0);
          result.set((int)(buf.sum/buf.count));
          return result;
        } else {
          LongWritable result = new LongWritable(0);
          result.set(buf.sum/buf.count);
          return result;
        }

      }
    }

    @Override
    public void reset(AggregationBuffer aggBuffer) throws HiveException {
      AverageAggBuf buf = (AverageAggBuf) aggBuffer;
      buf.count = 0;
      buf.sum = 0;
    }
  } // end of UDAFAverageEvaluatorLong

  // ===== GPU RapidsUDAF Implementation =====
  @Override
  public Scalar[] getDefaultValue() {
    // Return default values for [sum, count] - these need to match the
    // output of updateAggregation and also ideally match the output of
    // initialize in the CPU Hive version.
    Scalar sum = Scalar.fromNull(DType.INT64);
    try {
      Scalar count = Scalar.fromLong(0L);
      return new Scalar[]{sum, count};
    } catch (Exception e) {
      // Make sure 'sum' is closed if any exceptions after being created, to avoid
      // GPU memory leak.
      sum.close();
      throw e;
    }
  }

  // preProcess uses default implementation (pass-through) because no
  // change is needed to the single input column

  @Override
  public RapidsUDAFGroupByAggregation updateAggregation() {
    return new RapidsSimpleGroupByAggregation() {
      // "preStep" uses the default implementation (pass-through)

      @Override
      public Scalar[] reduce(int numRows, ColumnVector[] preStepData) {
        if (preStepData.length != 1) {
          throw new IllegalArgumentException("Expect only one column for update reduce.");
        }
        // For reduction (no group-by keys), compute SUM and COUNT directly
        ColumnVector inCol = preStepData[0];
        Scalar sum = preStepData[0].sum();
        try {
          Scalar count = Scalar.fromLong(inCol.getRowCount() - inCol.getNullCount());
          return new Scalar[]{sum, count};
        } catch (Exception e) {
          // Make sure that we don't leak if there is an exception.
          sum.close();
          throw e;
        }
      }

      @Override
      public GroupByAggregationOnColumn[] aggregate(int[] inputIndices) {
        if (inputIndices.length != 1) {
          throw new IllegalArgumentException("Expect only one column for update aggregate.");
        }
        // For group-by aggregation, create SUM and COUNT operations
        int colIndex = inputIndices[0];
        return new GroupByAggregationOnColumn[]{
            GroupByAggregation.sum().onColumn(colIndex),
            GroupByAggregation.count().onColumn(colIndex)
        };
      }

      @Override
      public ColumnVector[] postStep(ColumnVector[] aggregatedData) {
        // The original input is type of integer, and cudf count() aggregate also produce
        // an integer column, so convert them both to Long to match the agg buffer type.
        assert (aggregatedData.length == 2);
        try (ColumnVector sum = aggregatedData[0];
             ColumnVector count = aggregatedData[1]) {
          ColumnVector sumAsLong = sum.castTo(DType.INT64);
          try {
            ColumnVector countAsLong = count.castTo(DType.INT64);
            return new ColumnVector[] {sumAsLong, countAsLong};
          } catch (Exception e) {
            sumAsLong.close();
            throw e;
          }
        }
      }
    };
  }

  @Override
  public RapidsUDAFGroupByAggregation mergeAggregation() {
    return new RapidsSimpleGroupByAggregation() {
      // "preStep" uses the default implementation (pass-through)

      @Override
      public Scalar[] reduce(int numRows, ColumnVector[] preStepData) {
        if (preStepData.length != 2) {
          throw new IllegalArgumentException("Expect twos column for merge reduce.");
        }
        ColumnVector sumCol = preStepData[0];
        ColumnVector countCol = preStepData[1];
        Scalar sum = sumCol.sum();
        try {
          Scalar count = countCol.sum();
          return new Scalar[]{sum, count};
        } catch (Exception e) {
          // Make sure that we don't leak if there is an exception.
          sum.close();
          throw e;
        }
      }

      @Override
      public GroupByAggregationOnColumn[] aggregate(int[] inputIndices) {
        if (inputIndices.length != 2) {
          throw new IllegalArgumentException("Expect twos column for merge aggregate.");
        }
        return new GroupByAggregationOnColumn[]{
            GroupByAggregation.sum().onColumn(inputIndices[0]), // sum of sums
            GroupByAggregation.sum().onColumn(inputIndices[1])  // sum of counts
        };
      }

      // "postStep" uses the default implementation (pass-through)
    };
  }

  @Override
  public ColumnVector postProcess(int numRows, ColumnVector[] args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("Expect twos column for postProcess.");
    }
    // Final step: divide sum by count to get average
    try (ColumnVector sumCol = args[0];
        ColumnVector countCol = args[1]) {
      return sumCol.div(countCol);
    }
  }

  @Override
  public DataType[] aggBufferTypes() {
    return new DataType[]{LongType, LongType};
  }
}

