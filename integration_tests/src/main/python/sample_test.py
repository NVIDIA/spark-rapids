# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
from marks import *

_table_gen = [
    ('a', StringGen()),
    ('b', StringGen())]

@ignore_order
@pytest.mark.parametrize('data_gen', [_table_gen], ids=idfn)
def test_sample(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=2048).sample(0.9, 1)
    )

@ignore_order
@pytest.mark.parametrize('data_gen', [_table_gen], ids=idfn)
def test_sample_override(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).sample(True, 0.5, 1)
    )