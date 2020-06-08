# Copyright (c) 2020, NVIDIA CORPORATION.
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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from marks import incompat, ignore_order, allow_non_gpu, approximate_float

@ignore_order
def test_tpcxbb_q5(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q5"))

@incompat
@approximate_float
@ignore_order
@allow_non_gpu(any=True)
def test_tpcxbb_q16(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q16"))

@ignore_order
@allow_non_gpu(any=True)
def test_tpcxbb_q21(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q21"))

@ignore_order
@allow_non_gpu(any=True)
def test_tpcxbb_q22(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q22"))
