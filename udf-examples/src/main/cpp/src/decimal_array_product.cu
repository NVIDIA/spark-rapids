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

#include "decimal_array_product.hpp"

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
#include <rmm/exec_policy.hpp>

#include <thrust/iterator/counting_iterator.h>
#include <thrust/transform.h>


std::unique_ptr<cudf::column> decimal_array_product(cudf::lists_column_view const& arrays) {

  std::unique_ptr<cudf::column> result =
    cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::DECIMAL128, 0},
      strs.size(),
      std::move(null_mask),
      strs.null_count());

  auto stream = rmm::cuda_stream_default;
  auto offsets = arrays.offsets_begin();
  auto data = array.get_sliced_child(stream).data();
  thrust::transform(
    rmm::exec_policy(stream),
    thrust::make_counting_iterator<cudf::size_type>(0),
    thrust::make_counting_iterator<cudf::size_type>(arrays.size()),
    result->mutable_view().data<__int128_t>(),
    [offsets, data] __device__(cudf::size_type idx) {
       return thrust::reduce(thrust::seq,
         data[offsets[idx]], data[offsets[idx + 1]],
         (__int128_t) 1,
         thrust::multiplies<__int128_t>());
    }
  );

  return result;
}
