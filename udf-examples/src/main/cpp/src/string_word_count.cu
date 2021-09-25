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

#include "string_word_count.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_device_view.cuh>
#include <cudf/strings/string_view.cuh>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
#include <rmm/exec_policy.hpp>

#include <thrust/iterator/counting_iterator.h>
#include <thrust/transform.h>

namespace {

// count the words separated by whitespace characters
__device__ cudf::size_type count_words(cudf::column_device_view const& d_strings,
                                       cudf::size_type idx) {
  if (d_strings.is_null(idx)) return 0;
  cudf::string_view const d_str = d_strings.element<cudf::string_view>(idx);
  cudf::size_type word_count    = 0;
  // run of whitespace is considered a single delimiter
  bool spaces = true;
  auto itr    = d_str.begin();
  while (itr != d_str.end()) {
    cudf::char_utf8 ch = *itr;
    if (spaces == (ch <= ' ')) {
      itr++;
    } else {
      word_count += static_cast<cudf::size_type>(spaces);
      spaces = !spaces;
    }
  }

  return word_count;
}


} // anonymous namespace

/**
 * @brief Count the words in a string using whitespace as word boundaries
 *
 * @param strs The column containing the strings
 * @param stream The CUDA stream to use
 * @return The INT32 column containing the word count results per string
 */
std::unique_ptr<cudf::column> string_word_count(cudf::column_view const& strs) {
  auto strings_count = strs.size();
  if (strings_count == 0) {
    return cudf::make_empty_column(cudf::data_type{cudf::type_id::INT32});
  }

  // the validity of the output matches the validity of the input
  rmm::device_buffer null_mask = cudf::copy_bitmask(strs);

  // allocate the column that will contain the word count results
  std::unique_ptr<cudf::column> result =
    cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      strs.size(),
      std::move(null_mask),
      strs.null_count());

  // compute the word counts, writing into the result column data buffer
  auto stream = rmm::cuda_stream_default;
  auto strs_device_view = cudf::column_device_view::create(strs, stream);
  auto d_strs_view = *strs_device_view;
  thrust::transform(
    rmm::exec_policy(stream),
    thrust::make_counting_iterator<cudf::size_type>(0),
    thrust::make_counting_iterator<cudf::size_type>(strings_count),
    result->mutable_view().data<cudf::size_type>(),
    [d_strs_view] __device__(cudf::size_type idx) { return count_words(d_strs_view, idx); });

  return result;
}
