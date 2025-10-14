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

package com.nvidia.spark;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;
import org.apache.spark.sql.types.DataType;

/**
 * An interface for a GPU-accelerated User Defined Aggregate Function (UDAF).
 * This provides the necessary methods to perform distributed group-by and
 * reduction aggregations using CUDF.
 */
public interface RapidsUDAF {

  /**
   * Provides an array of default values for the aggregation result. This is
   * used when a reduction aggregation does not have any rows to aggregate.
   * <br/>
   * Rapids will close these Scalars after being converted to columns.
   * <br/>
   * @return An array of cudf Scalar representing the output of the
   *         updateAggregation stage of processing. The output of this
   *         may still be merged with other tasks.
   */
  Scalar[] getDefaultValue();

  /**
   * A pre-processing step that transforms the input ColumnVector arguments.
   * This method is similar to a regular RapidsUDF but returns an array of
   * ColumnVectors. By default, this is a no-op and will just return the
   * arguments passed in.
   * <br/>
   * Users should close the input columns to avoid GPU memory leak, while the
   * returned columns will be closed by the Rapids automatically.
   *
   * @param numRows The number of rows to process. This is for cases
   *               like a `COUNT(*)`, where there may be no arguments to a UDAF.
   *               This is not common.
   * @param args An array of ColumnVector arguments.
   * @return An array of ColumnVectors representing the pre-processed data.
   */
  default ColumnVector[] preProcess(int numRows, ColumnVector[] args) {
    return args;
  }

  /**
   * This method returns a RapidsUDAFGroupByAggregation that defines the
   * logic for the initial aggregation. The preProcess method will be called
   * first, and its output will then be processed by the
   * RapidsUDAFGroupByAggregation that this method returns.
   * <br/>
   * @return A RapidsUDAFGroupByAggregation that defines the aggregation
   * logic.
   */
  RapidsUDAFGroupByAggregation updateAggregation();

  /**
   * This method returns a RapidsUDAFGroupByAggregation that defines how to
   * merge two sets of aggregation results. This is used in distributed
   * aggregation scenarios where intermediate results from different
   * partitions are combined.
   * <br/>
   * @return A RapidsUDAFGroupByAggregation that defines the merge logic.
   */
  RapidsUDAFGroupByAggregation mergeAggregation();

  /**
   * A post-processing step that takes the result of the final aggregation
   * and performs any necessary transformations before returning the final
   * result. This method returns a single ColumnVector, which is the final
   * result of the aggregation.
   * <br/>
   * Users should close the input columns to avoid GPU memory leak. But the
   * returned column will be closed by the Rapids automatically.
   * <br/>
   * @param numRows The number of rows in the aggregated data.
   * @param args An array of ColumnVector arguments from the final aggregation step.
   * @param outType The final data type of this UDAF
   * @return A single ColumnVector representing the final UDAF result.
   */
  ColumnVector postProcess(int numRows, ColumnVector[] args, DataType outType);

  /**
   * Data types of the aggregate buffer. This is similar as the "bufferSchema" of
   * class UserDefinedAggregateFunction in Spark.
   */
  DataType[] aggBufferTypes();
}
