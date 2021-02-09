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
cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(UDFEXAMPLESJNI VERSION 0.5.0 LANGUAGES C CXX CUDA)

###################################################################################################
# - build type ------------------------------------------------------------------------------------

# Set a default build type if none was specified
set(DEFAULT_BUILD_TYPE "Release")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' since none specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

###################################################################################################
# - compiler options ------------------------------------------------------------------------------

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_C_COMPILER $ENV{CC})
set(CMAKE_CXX_COMPILER $ENV{CXX})
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wno-error=deprecated-declarations")
endif(CMAKE_COMPILER_IS_GNUCXX)

if(CMAKE_CUDA_COMPILER_VERSION)
  # Compute the version from CMAKE_CUDA_COMPILER_VERSION
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\1" CUDA_VERSION_MAJOR ${CMAKE_CUDA_COMPILER_VERSION})
  string(REGEX REPLACE "([0-9]+)\\.([0-9]+).*" "\\2" CUDA_VERSION_MINOR ${CMAKE_CUDA_COMPILER_VERSION})
  set(CUDA_VERSION "${CUDA_VERSION_MAJOR}.${CUDA_VERSION_MINOR}" CACHE STRING "Version of CUDA as computed from nvcc.")
  mark_as_advanced(CUDA_VERSION)
endif()

message(STATUS "CUDA_VERSION_MAJOR: ${CUDA_VERSION_MAJOR}")
message(STATUS "CUDA_VERSION_MINOR: ${CUDA_VERSION_MINOR}")
message(STATUS "CUDA_VERSION: ${CUDA_VERSION}")

# Always set this convenience variable
set(CUDA_VERSION_STRING "${CUDA_VERSION}")

set(GPU_ARCHS "ALL" CACHE STRING
  "List of GPU architectures (semicolon-separated) to be compiled for. Pass 'ALL' if you want to compile for all supported GPU architectures.")

if("${GPU_ARCHS}" STREQUAL "ALL")

  # Check for embedded vs workstation architectures
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    # This is being built for Linux4Tegra or SBSA ARM64
    set(GPU_ARCHS "62")
    if((CUDA_VERSION_MAJOR EQUAL 9) OR (CUDA_VERSION_MAJOR GREATER 9))
      set(GPU_ARCHS "${GPU_ARCHS};72")
    endif()
    if((CUDA_VERSION_MAJOR EQUAL 11) OR (CUDA_VERSION_MAJOR GREATER 11))
      set(GPU_ARCHS "${GPU_ARCHS};75;80")
    endif()

  else()
    # This is being built for an x86 or x86_64 architecture
    set(GPU_ARCHS "60")
    if((CUDA_VERSION_MAJOR EQUAL 9) OR (CUDA_VERSION_MAJOR GREATER 9))
      set(GPU_ARCHS "${GPU_ARCHS};70")
    endif()
    if((CUDA_VERSION_MAJOR EQUAL 10) OR (CUDA_VERSION_MAJOR GREATER 10))
      set(GPU_ARCHS "${GPU_ARCHS};75")
    endif()
    if((CUDA_VERSION_MAJOR EQUAL 11) OR (CUDA_VERSION_MAJOR GREATER 11))
      set(GPU_ARCHS "${GPU_ARCHS};80")
    endif()

  endif()

endif()
message("GPU_ARCHS = ${GPU_ARCHS}")

foreach(arch ${GPU_ARCHS})
  set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -gencode=arch=compute_${arch},code=sm_${arch}")
endforeach()

set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-extended-lambda --expt-relaxed-constexpr")

# set warnings as errors
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Werror cross-execution-space-call -Xcompiler -Wall,-Werror,-Wno-error=deprecated-declarations")

# Option to enable line info in CUDA device compilation to allow introspection when profiling / memchecking
option(CMAKE_CUDA_LINEINFO "Enable the -lineinfo option for nvcc (useful for cuda-memcheck / profiler)" OFF)
if (CMAKE_CUDA_LINEINFO)
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -lineinfo")
endif(CMAKE_CUDA_LINEINFO)

# Debug options
if(CMAKE_BUILD_TYPE MATCHES Debug)
    message(STATUS "Building with debugging flags")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -G -Xcompiler -rdynamic")
endif(CMAKE_BUILD_TYPE MATCHES Debug)

option(CUDA_STATIC_RUNTIME "Statically link the CUDA runtime" ON)

if(CUDA_STATIC_RUNTIME)
    message(STATUS "Enabling static linking of cudart")
    set(CUDART_LIBRARY "cudart_static")
else()
    message(STATUS "Enabling dynamic linking of cudart")
    set(CUDART_LIBRARY "cudart")
endif(CUDA_STATIC_RUNTIME)

###################################################################################################
# - CUDF ------------------------------------------------------------------------------------------

find_path(CUDF_INCLUDE "cudf"
    HINTS "$ENV{CUDF_ROOT}"
          "$ENV{CONDA_PREFIX}/include")

find_library(CUDF_BASE_LIBRARY "cudf_base"
    HINTS "$ENV{CUDF_ROOT}"
          "$ENV{CUDF_ROOT}/lib"
          "$ENV{CONDA_PREFIX}/lib")

###################################################################################################
# - RMM -------------------------------------------------------------------------------------------

find_path(RMM_INCLUDE "rmm"
          HINTS "$ENV{RMM_ROOT}/include"
                "$ENV{CONDA_PREFIX}/include")

###################################################################################################
# - Thrust/CUB/libcudacxx ------------------------------------------------------------------------------------

# path to libcudf cmake build directory
if(NOT DEFINED CUDF_CMAKE_BUILD_DIR)
  set(CUDF_CMAKE_BUILD_DIR "/cudf/cpp/build")
endif(NOT DEFINED CUDF_CMAKE_BUILD_DIR)

find_path(THRUST_INCLUDE "thrust"
    HINTS "$ENV{CUDF_ROOT}/_deps/thrust-src"
          "${CUDF_CMAKE_BUILD_DIR}/_deps/thrust-src")

find_path(CUB_INCLUDE "cub"
    HINTS "$ENV{CUDF_ROOT}/_deps/thrust-src"
          "${CUDF_CMAKE_BUILD_DIR}/_deps/thrust-src")

find_path(LIBCUDACXX_INCLUDE "cuda"
    HINTS "$ENV{CUDF_ROOT}/_deps/libcudacxx-src/include"
          "${CUDF_CMAKE_BUILD_DIR}/_deps/libcudacxx-src/include")

###################################################################################################
# - find JNI -------------------------------------------------------------------------------------

find_package(JNI REQUIRED)
if(JNI_FOUND)
    message(STATUS "JDK with JNI in ${JNI_INCLUDE_DIRS}")
else()
    message(FATAL_ERROR "JDK with JNI not found, please check your settings.")
endif(JNI_FOUND)

###################################################################################################
# - include paths ---------------------------------------------------------------------------------

include_directories("${THRUST_INCLUDE}"
                    "${CUB_INCLUDE}"
                    "${LIBCUDACXX_INCLUDE}"
                    "${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}"
                    "${CMAKE_SOURCE_DIR}/src"
                    "${CUDF_INCLUDE}"
                    "${RMM_INCLUDE}"
                    "${JNI_INCLUDE_DIRS}")

###################################################################################################
# - library paths ---------------------------------------------------------------------------------

# CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES is an undocumented/unsupported variable containing the link directories for nvcc
link_directories("${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES}"
                 "${CMAKE_BINARY_DIR}/lib")


###################################################################################################
# - library targets -------------------------------------------------------------------------------

set(SOURCE_FILES
    "src/StringWordCountJni.cpp"
    "src/string_word_count.cu")

add_library(udfexamplesjni SHARED ${SOURCE_FILES})

#Override RPATH for udfexamplesjni
SET_TARGET_PROPERTIES(udfexamplesjni PROPERTIES BUILD_RPATH "\$ORIGIN")

###################################################################################################
# - build options ---------------------------------------------------------------------------------

option(PER_THREAD_DEFAULT_STREAM "Build with per-thread default stream" OFF)
if(PER_THREAD_DEFAULT_STREAM)
    message(STATUS "Using per-thread default stream")
    add_compile_definitions(CUDA_API_PER_THREAD_DEFAULT_STREAM)
endif(PER_THREAD_DEFAULT_STREAM)

###################################################################################################
# - rmm logging level -----------------------------------------------------------------------------

set(RMM_LOGGING_LEVEL "INFO" CACHE STRING "Choose the logging level.")
# Set the possible values of build type for cmake-gui
set_property(CACHE RMM_LOGGING_LEVEL PROPERTY STRINGS
        "TRACE" "DEBUG" "INFO" "WARN" "ERROR" "CRITICAL" "OFF")
message(STATUS "RMM_LOGGING_LEVEL = '${RMM_LOGGING_LEVEL}'.")

target_compile_definitions(udfexamplesjni
    PUBLIC SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_${RMM_LOGGING_LEVEL})

###################################################################################################
# - link libraries --------------------------------------------------------------------------------

target_link_libraries(udfexamplesjni ${CUDF_BASE_LIBRARY} ${CUDART_LIBRARY} cuda)
