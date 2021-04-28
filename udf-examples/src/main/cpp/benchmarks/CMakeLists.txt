#=============================================================================
# Copyright (c) 2021, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#=============================================================================

# Use an OBJECT library so we only compile these helper source files only once
add_library(udf_benchmark_common OBJECT
    synchronization/synchronization.cpp)

target_link_libraries(udf_benchmark_common PUBLIC benchmark::benchmark cudf)

target_include_directories(udf_benchmark_common
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${UDFEXAMPLESJNI_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${UDFEXAMPLESJNI_SOURCE_DIR}>/src")

function(ConfigureBench CMAKE_BENCH_NAME)
    add_executable(${CMAKE_BENCH_NAME} ${ARGN})
    set_target_properties(${CMAKE_BENCH_NAME}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "$<BUILD_INTERFACE:${UDFEXAMPLESJNI_BINARY_DIR}/gbenchmarks>")
    target_link_libraries(${CMAKE_BENCH_NAME}
        PRIVATE udf_benchmark_common udfexamplesjni benchmark::benchmark_main)
endfunction()

ConfigureBench(COSINE_SIMILARITY_BENCH cosine_similarity/cosine_similarity_benchmark.cpp)
