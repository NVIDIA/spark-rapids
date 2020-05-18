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
from pyspark.sql.types import *
from marks import ignore_order
import pyspark.sql.functions as f

gens_for_hash = [
    RepeatSeqGen(LongGen(nullable=False, min_val = 0, max_val = 50), length = 100),
    RepeatSeqGen(LongGen(nullable=True, min_val = 0, max_val = 50), length = 100),
    RepeatSeqGen(IntegerGen(nullable=False, min_val = 0, max_val = 150), length = 100),
    RepeatSeqGen(IntegerGen(nullable=True, min_val = 0, max_val = 150), length = 100)
]

@ignore_order
@pytest.mark.parametrize('data_gen', gens_for_hash, ids=idfn)
def test_hash_grpby_sum(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen, length=100).groupby('a').agg(f.sum('b'))
    )