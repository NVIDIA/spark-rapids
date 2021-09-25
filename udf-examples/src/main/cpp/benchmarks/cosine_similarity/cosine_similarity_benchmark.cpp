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

#include "benchmarks/fixture/benchmark_fixture.hpp"
#include "benchmarks/synchronization/synchronization.hpp"
#include "cosine_similarity.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/scalar/scalar_factories.hpp>

static void cosine_similarity_bench_args(benchmark::internal::Benchmark* b)
{
  int const min_rows   = 1 << 12;
  int const max_rows   = 1 << 24;
  int const row_mult   = 8;
  int const min_rowlen = 1 << 0;
  int const max_rowlen = 1 << 12;
  int const len_mult   = 8;
  for (int row_count = min_rows; row_count <= max_rows; row_count *= row_mult) {
    for (int rowlen = min_rowlen; rowlen <= max_rowlen; rowlen *= len_mult) {
      // avoid generating combinations that exceed the cudf column limit
      size_t total_chars = static_cast<size_t>(row_count) * rowlen;
      if (total_chars < std::numeric_limits<cudf::size_type>::max()) {
        b->Args({row_count, rowlen});
      }
    }
  }
}

static void BM_cosine_similarity(benchmark::State& state)
{
  cudf::size_type const n_rows{static_cast<cudf::size_type>(state.range(0))};
  cudf::size_type const list_len{static_cast<cudf::size_type>(state.range(1))};

  auto val_start = cudf::make_fixed_width_scalar(1.0f);
  auto val_step = cudf::make_fixed_width_scalar(-1.0f);
  auto child_rows = n_rows * list_len;
  auto col1_child = cudf::sequence(child_rows, *val_start);
  auto col2_child = cudf::sequence(child_rows, *val_start, *val_step);
  auto offset_start = cudf::make_fixed_width_scalar(static_cast<int32_t>(0));
  auto offset_step = cudf::make_fixed_width_scalar(list_len);
  auto offsets = cudf::sequence(n_rows + 1, *offset_start, *offset_step);

  auto col1 = cudf::make_lists_column(
      n_rows,
      std::make_unique<cudf::column>(*offsets),
      std::move(col1_child),
      0,
      cudf::create_null_mask(n_rows, cudf::mask_state::ALL_VALID));
  auto lcol1 = cudf::lists_column_view(*col1);
  auto col2 = cudf::make_lists_column(
      n_rows,
      std::move(offsets),
      std::move(col2_child),
      0,
      cudf::create_null_mask(n_rows, cudf::mask_state::ALL_VALID));
  auto lcol2 = cudf::lists_column_view(*col2);

  for (auto _ : state) {
    cuda_event_timer raii(state, true, rmm::cuda_stream_default);
    auto output = cosine_similarity(lcol1, lcol2);
  }

  state.SetBytesProcessed(state.iterations() * child_rows * sizeof(float));
}

class CosineSimilarity : public native_udf::benchmark {
};

BENCHMARK_DEFINE_F(CosineSimilarity, cosine_similarity)
(::benchmark::State& state) { BM_cosine_similarity(state); }

BENCHMARK_REGISTER_F(CosineSimilarity, cosine_similarity)
  ->Apply(cosine_similarity_bench_args)
  ->Unit(benchmark::kMillisecond)
  ->UseManualTime();
