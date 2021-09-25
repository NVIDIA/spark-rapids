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
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f


# This is one of the most basic tests where we verify that we can
# move data onto and off of the GPU without messing up. All data
# that comes from data_gen is row formatted, with how pyspark
# currently works and when we do a collect all of that data has
# to be brought back to the CPU (rows) to be returned.
# So we just need a very simple operation in the middle that
# can be done on the GPU.
def test_row_conversions():
    gens = [["a", byte_gen], ["b", short_gen], ["c", int_gen], ["d", long_gen],
            ["e", float_gen], ["f", double_gen], ["g", string_gen], ["h", boolean_gen],
            ["i", timestamp_gen], ["j", date_gen], ["k", ArrayGen(byte_gen)],
            ["l", ArrayGen(string_gen)], ["m", ArrayGen(float_gen)],
            ["n", ArrayGen(boolean_gen)], ["o", ArrayGen(ArrayGen(short_gen))],
            ["p", StructGen([["c0", byte_gen], ["c1", ArrayGen(byte_gen)]])],
            ["q", simple_string_to_string_map_gen],
            ["r", MapGen(BooleanGen(nullable=False), ArrayGen(boolean_gen), max_length=2)],
            ["s", null_gen], ["t", decimal_gen_64bit], ["u", decimal_gen_scale_precision]]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gens).selectExpr("*", "a as a_again"))

def test_row_conversions_fixed_width():
    gens = [["a", byte_gen], ["b", short_gen], ["c", int_gen], ["d", long_gen],
            ["e", float_gen], ["f", double_gen], ["g", string_gen], ["h", boolean_gen],
            ["i", timestamp_gen], ["j", date_gen], ["k", decimal_gen_64bit],
            ["l", decimal_gen_scale_precision]]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gens).selectExpr("*", "a as a_again"))
