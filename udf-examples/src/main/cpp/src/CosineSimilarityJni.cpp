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

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/lists/lists_column_view.hpp>

#include <memory>
#include <jni.h>

#include "cosine_similarity.hpp"

namespace {

constexpr char const* RUNTIME_ERROR_CLASS = "java/lang/RuntimeException";
constexpr char const* ILLEGAL_ARG_CLASS   = "java/lang/IllegalArgumentException";

/**
 * @brief Throw a Java exception
 *
 * @param env The Java environment
 * @param class_name The fully qualified Java class name of the exception
 * @param msg The message string to associate with the exception
 */
void throw_java_exception(JNIEnv* env, char const* class_name, char const* msg) {
  jclass ex_class = env->FindClass(class_name);
  if (ex_class != NULL) {
    env->ThrowNew(ex_class, msg);
  }
}

}  // anonymous namespace

extern "C" {

/**
 * @brief The native implementation of CosineSimilarity.cosineSimilarity which
 * computes the cosine similarity between two LIST(FLOAT32) columns as a FLOAT32
 * columnar result.
 *
 * @param env The Java environment
 * @param j_view1 The address of the cudf column view of the first LIST column
 * @param j_view2 The address of the cudf column view of the second LIST column
 * @return The address of the cudf column containing the FLOAT32 results
 */
JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_udf_java_CosineSimilarity_cosineSimilarity(JNIEnv* env, jclass,
                                                                        jlong j_view1,
                                                                        jlong j_view2) {
  // Use a try block to translate C++ exceptions into Java exceptions to avoid
  // crashing the JVM if a C++ exception occurs.
  try {
    // turn the addresses into column_view pointers
    auto v1 = reinterpret_cast<cudf::column_view const*>(j_view1);
    auto v2 = reinterpret_cast<cudf::column_view const*>(j_view2);
    if (v1->type().id() != v2->type().id() || v1->type().id() != cudf::type_id::LIST) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "inputs not list columns");
      return 0;
    }

    // run the GPU kernel to compute the cosine similarity
    auto lv1 = cudf::lists_column_view(*v1);
    auto lv2 = cudf::lists_column_view(*v2);
    std::unique_ptr<cudf::column> result = cosine_similarity(lv1, lv2);

    // take ownership of the column and return the column address to Java
    return reinterpret_cast<jlong>(result.release());
  } catch (std::bad_alloc const& e) {
    auto msg = std::string("Unable to allocate native memory: ") +
        (e.what() == nullptr ? "" : e.what());
    throw_java_exception(env, RUNTIME_ERROR_CLASS, msg.c_str());
  } catch (std::invalid_argument const& e) {
    throw_java_exception(env, ILLEGAL_ARG_CLASS, e.what() == nullptr ? "" : e.what());
  } catch (std::exception const& e) {
    auto msg = e.what() == nullptr ? "" : e.what();
    throw_java_exception(env, RUNTIME_ERROR_CLASS, msg);
  }
  return 0;
}

}
