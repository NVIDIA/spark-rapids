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
from marks import incompat
from pyspark.sql.types import *
import pyspark.sql.functions as f

@pytest.mark.parametrize('data_gen', [simple_string_to_string_map_gen], ids=idfn)
def test_simple_get_map_value(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a["key_0"]',
                'a["key_1"]',
                'a[null]',
                'a["key_9"]',
                'a["NOT_FOUND"]',
                'a["key_5"]'))
