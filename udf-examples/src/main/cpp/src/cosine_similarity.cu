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

#include "cosine_similarity.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_device_view.cuh>
#include <cudf/lists/list_device_view.cuh>
#include <cudf/lists/lists_column_device_view.cuh>
#include <cudf/lists/lists_column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/table/table_view.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
#include <rmm/exec_policy.hpp>

#include <thrust/iterator/counting_iterator.h>
#include <thrust/logical.h>
#include <thrust/transform.h>

#include <cmath>

namespace {

/**
 * @brief Functor for computing the cosine similarity between two list of float columns
 */
struct cosine_similarity_functor {
  float const* const v1;
  float const* const v2;
  int32_t const* const v1_offsets;
  int32_t const* const v2_offsets;

  // This kernel executes thread-per-row which should be fine for relatively short lists
  // but may need to be revisited for performance if operating on long lists.
  __device__ float operator()(cudf::size_type row_idx) {
    auto const v1_start_idx = v1_offsets[row_idx];
    auto const v1_num_elems = v1_offsets[row_idx + 1] - v1_start_idx;
    auto const v2_start_idx = v2_offsets[row_idx];
    auto const v2_num_elems = v2_offsets[row_idx + 1] - v2_start_idx;
    auto const num_elems = std::min(v1_num_elems, v2_num_elems);
    double mag1 = 0;
    double mag2 = 0;
    double dot_product = 0;
    for (auto i = 0; i < num_elems; i++) {
      float const f1 = v1[v1_start_idx + i];
      mag1 += f1 * f1;
      float const f2 = v2[v2_start_idx + i];
      mag2 += f2 * f2;
      dot_product += f1 * f2;
    }
    mag1 = std::sqrt(mag1);
    mag2 = std::sqrt(mag2);
    return static_cast<float>(dot_product / (mag1 * mag2));
  }
};

} // anonymous namespace

/**
 * @brief Compute the cosine similarity between two LIST of FLOAT32 columns
 *
 * The input vectors must have matching shapes, i.e.: same row count and same number of
 * list elements per row. A null list row is supported, but null float entries within a
 * list are not supported.
 *
 * @param lv1 The first LIST of FLOAT32 column view
 * @param lv2 The second LIST of FLOAT32 column view
 * @return A FLOAT32 column containing the cosine similarity corresponding to each input row
 */
std::unique_ptr<cudf::column> cosine_similarity(cudf::lists_column_view const& lv1,
                                                cudf::lists_column_view const& lv2) {
  // sanity-check the input types
  if (lv1.child().type().id() != lv2.child().type().id() ||
      lv1.child().type().id() != cudf::type_id::FLOAT32) {
    throw std::invalid_argument("inputs are not lists of floats");
  }

  // sanity check the input shape
  auto const row_count = lv1.size();
  if (row_count != lv2.size()) {
    throw std::invalid_argument("input row counts do not match");
  }
  if (row_count == 0) {
    return cudf::make_empty_column(cudf::data_type{cudf::type_id::FLOAT32});
  }
  if (lv1.child().null_count() != 0 || lv2.child().null_count() != 0) {
    throw std::invalid_argument("null floats are not supported");
  }

  auto const stream = rmm::cuda_stream_default;
  auto d_view1_ptr = cudf::column_device_view::create(lv1.parent());
  auto d_lists1 = cudf::detail::lists_column_device_view(*d_view1_ptr);
  auto d_view2_ptr = cudf::column_device_view::create(lv2.parent());
  auto d_lists2 = cudf::detail::lists_column_device_view(*d_view2_ptr);
  bool const are_offsets_equal =
    thrust::all_of(rmm::exec_policy(stream),
                   thrust::make_counting_iterator<cudf::size_type>(0),
                   thrust::make_counting_iterator<cudf::size_type>(row_count),
                   [d_lists1, d_lists2] __device__(cudf::size_type idx) {
                     auto ldv1 = cudf::list_device_view(d_lists1, idx);
                     auto ldv2 = cudf::list_device_view(d_lists2, idx);
                     return ldv1.is_null() || ldv2.is_null() || ldv1.size() == ldv2.size();
                   });
  if (not are_offsets_equal) {
    throw std::invalid_argument("input list lengths do not match for every row");
  }

  // allocate the vector of float results
  rmm::device_uvector<float> float_results(row_count, stream);

  // compute the cosine similarity
  auto const lv1_data = lv1.child().data<float>();
  auto const lv2_data = lv2.child().data<float>();
  auto const lv1_offsets = lv1.offsets().data<int32_t>();
  auto const lv2_offsets = lv2.offsets().data<int32_t>();
  thrust::transform(rmm::exec_policy(stream),
                    thrust::make_counting_iterator<cudf::size_type>(0),
                    thrust::make_counting_iterator<cudf::size_type>(row_count),
                    float_results.data(),
                    cosine_similarity_functor({lv1_data, lv2_data, lv1_offsets, lv2_offsets}));

  // the validity of the output is the bitwise-and of the two input validity masks
  rmm::device_buffer null_mask = cudf::bitmask_and(cudf::table_view({lv1.parent(), lv2.parent()}));

  return std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::FLOAT32},
                                        row_count,
                                        float_results.release(),
                                        std::move(null_mask));
}
