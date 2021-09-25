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

#include <memory>
#include <jni.h>

#include "string_word_count.hpp"

namespace {

constexpr char const* RUNTIME_ERROR_CLASS = "java/lang/RuntimeException";

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
 * @brief The native implementation of StringWordCount.countWords which counts the
 * number of words per string in a string column.
 *
 * @param env The Java environment
 * @param j_strings The address of the cudf column view of the strings column
 * @return The address of the cudf column containing the word counts
 */
JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_udf_hive_StringWordCount_countWords(JNIEnv* env, jclass,
                                                                 jlong j_strings) {
  // Use a try block to translate C++ exceptions into Java exceptions to avoid
  // crashing the JVM if a C++ exception occurs.
  try {
    // turn the addresses into column_view pointers
    auto strs = reinterpret_cast<cudf::column_view const*>(j_strings);

    // run the GPU kernel to compute the word counts
    std::unique_ptr<cudf::column> result = string_word_count(*strs);

    // take ownership of the column and return the column address to Java
    return reinterpret_cast<jlong>(result.release());
  } catch (std::bad_alloc const& e) {
    auto msg = std::string("Unable to allocate native memory: ") +
        (e.what() == nullptr ? "" : e.what());
    throw_java_exception(env, RUNTIME_ERROR_CLASS, msg.c_str());
  } catch (std::exception const& e) {
    auto msg = e.what() == nullptr ? "" : e.what();
    throw_java_exception(env, RUNTIME_ERROR_CLASS, msg);
  }
  return 0;
}

}
