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

#pragma once

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/lists/lists_column_view.hpp>

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
                                                cudf::lists_column_view const& lv2);
