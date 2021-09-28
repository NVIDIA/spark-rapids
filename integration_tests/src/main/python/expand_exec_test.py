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
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_equal
from data_gen import *
import pyspark.sql.functions as f
from marks import ignore_order

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@ignore_order
def test_expand_exec(data_gen):
    def op_df(spark, length=20, seed=0):
        return gen_df(spark, StructGen([
            ('a', data_gen),
            ('b', IntegerGen())], nullable=False), length=length, seed=seed).rollup(f.col("a"), f.col("b")).agg(f.col("b"))

    assert_gpu_and_cpu_are_equal_collect(op_df)
