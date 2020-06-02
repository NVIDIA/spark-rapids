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
from data_gen import *

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
         FloatGen(), DoubleGen(), BooleanGen(), DateGen(), TimestampGen()]

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_union(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).union(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_union_by_name(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).unionByName(binary_op_df(spark, data_gen)))