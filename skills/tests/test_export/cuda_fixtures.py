# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
CUDA/CPP source fixtures for template integration tests.
"""

JNI_SOURCE = """\
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
    "native/src/main/cpp/src/IntegerMultiplyBy2Jni.cpp": JNI_SOURCE,
    "native/src/main/cpp/src/integer_multiply_by_2.cu": CUDA_SOURCE,
    "native/src/main/cpp/src/integer_multiply_by_2.hpp": HEADER_SOURCE,
}
