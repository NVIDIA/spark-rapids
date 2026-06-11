# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
CUDA source fixtures for JVM export integration tests.
"""

NATIVE_RAPIDS_UDF_SOURCE = """\
package com.udf;

import ai.rapids.cudf.ColumnVector;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;

public class IntegerMultiplyBy2NativeRapidsUDF extends UDF
        implements UDF1<Integer, Integer>, RapidsUDF {
    public Integer evaluate(Integer value) {
        if (value == null) return null;
        return value * 2;
    }

    @Override
    public Integer call(Integer value) {
        return evaluate(value);
    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Unexpected argument count: " + args.length);
        }
        if (numRows != args[0].getRowCount()) {
            throw new IllegalArgumentException(
                "Expected " + numRows + " rows, received " + args[0].getRowCount());
        }

        NativeUDFLoader.ensureLoaded();
        return new ColumnVector(integerMultiplyBy2(args[0].getNativeView()));
    }

    private static native long integerMultiplyBy2(long inputView);
}
"""

JNI_SOURCE = """\
// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

#include "integer_multiply_by_2.hpp"

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

#include <jni.h>

#include <memory>
#include <string>

namespace {

constexpr char const* RUNTIME_ERROR_CLASS = "java/lang/RuntimeException";
constexpr char const* ILLEGAL_ARG_CLASS = "java/lang/IllegalArgumentException";

void throw_java_exception(JNIEnv* env, char const* class_name, char const* message)
{
  jclass ex_class = env->FindClass(class_name);
  if (ex_class != nullptr) {
    env->ThrowNew(ex_class, message);
  }
}

}  // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_udf_IntegerMultiplyBy2NativeRapidsUDF_integerMultiplyBy2(JNIEnv* env,
                                                                  jclass,
                                                                  jlong input_view)
{
  try {
    auto input = reinterpret_cast<cudf::column_view const*>(input_view);
    if (input == nullptr) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "input column view is null");
      return 0;
    }
    if (input->type().id() != cudf::type_id::INT32) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "input must be INT32");
      return 0;
    }

    std::unique_ptr<cudf::column> result = integer_multiply_by_2(*input);
    return reinterpret_cast<jlong>(result.release());
  } catch (std::bad_alloc const& e) {
    auto message = std::string("Unable to allocate native memory: ") + e.what();
    throw_java_exception(env, RUNTIME_ERROR_CLASS, message.c_str());
  } catch (std::invalid_argument const& e) {
    throw_java_exception(env, ILLEGAL_ARG_CLASS, e.what());
  } catch (std::exception const& e) {
    throw_java_exception(env, RUNTIME_ERROR_CLASS, e.what());
  }
  return 0;
}

}
"""

CUDA_SOURCE = """\
// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

#include "integer_multiply_by_2.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/memory_resource.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/resource_ref.hpp>

#include <cstdint>
#include <stdexcept>

namespace {

__global__ void multiply_by_2_kernel(int32_t const* input, int32_t* output, cudf::size_type size)
{
  auto const idx = static_cast<cudf::size_type>(blockIdx.x * blockDim.x + threadIdx.x);
  if (idx < size) {
    output[idx] = input[idx] * 2;
  }
}

}  // namespace

std::unique_ptr<cudf::column> integer_multiply_by_2(
  cudf::column_view const& input,
  rmm::cuda_stream_view stream,
  rmm::device_async_resource_ref mr)
{
  if (input.type().id() != cudf::type_id::INT32) {
    throw std::invalid_argument("input must be INT32");
  }

  auto const row_count = input.size();
  auto null_mask       = cudf::copy_bitmask(input, stream, mr);
  auto result          = cudf::make_numeric_column(
    input.type(), row_count, std::move(null_mask), input.null_count(), stream, mr);

  if (row_count > 0) {
    constexpr int threads_per_block = 256;
    int const blocks = (row_count + threads_per_block - 1) / threads_per_block;
    multiply_by_2_kernel<<<blocks, threads_per_block, 0, stream.value()>>>(
      input.data<int32_t>(), result->mutable_view().data<int32_t>(), row_count);
    CUDF_CHECK_CUDA(stream.value());
  }

  return result;
}
"""

HEADER_SOURCE = """\
// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/memory_resource.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/resource_ref.hpp>

#include <memory>

std::unique_ptr<cudf::column> integer_multiply_by_2(
  cudf::column_view const& input,
  rmm::cuda_stream_view stream      = cudf::get_default_stream(),
  rmm::device_async_resource_ref mr = cudf::get_current_device_resource_ref());
"""

CMAKE_SOURCE_FILES = """\
set(SOURCE_FILES
  "src/IntegerMultiplyBy2Jni.cpp"
  "src/integer_multiply_by_2.cu"
)
"""

PLACEHOLDER_FILES = (
    "src/main/java/com/udf/PlaceholderUDFNameNativeRapidsUDF.java",
    "native/src/main/cpp/src/PlaceholderUDFNameJni.cpp",
    "native/src/main/cpp/src/placeholder_udf_name.cu",
    "native/src/main/cpp/src/placeholder_udf_name.hpp",
)

NATIVE_SOURCE_FILES = {
    "src/main/java/com/udf/IntegerMultiplyBy2NativeRapidsUDF.java": NATIVE_RAPIDS_UDF_SOURCE,
    "native/src/main/cpp/src/IntegerMultiplyBy2Jni.cpp": JNI_SOURCE,
    "native/src/main/cpp/src/integer_multiply_by_2.cu": CUDA_SOURCE,
    "native/src/main/cpp/src/integer_multiply_by_2.hpp": HEADER_SOURCE,
}
