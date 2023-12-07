# Copyright (c) 2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from conftest import is_not_utc
from data_gen import *
from marks import allow_non_gpu, ignore_order
from spark_session import is_before_spark_320

# Spark 3.1.x does not normalize -0.0 and 0.0 but GPU version does
_xxhash_gens = [
    null_gen,
    boolean_gen,
    byte_gen,
    short_gen,
    int_gen,
    long_gen,
    date_gen,
    timestamp_gen,
    decimal_gen_32bit,
    decimal_gen_64bit,
    decimal_gen_128bit]
if not is_before_spark_320():
    _xxhash_gens += [float_gen, double_gen]

_struct_of_xxhash_gens = StructGen([(f"c{i}", g) for i, g in enumerate(_xxhash_gens)])

_xxhash_fallback_gens = single_level_array_gens + nested_array_gens_sample + [
    all_basic_struct_gen,
    struct_array_gen,
    _struct_of_xxhash_gens]
if is_before_spark_320():
    _xxhash_fallback_gens += [float_gen, double_gen]

@ignore_order(local=True)
@pytest.mark.parametrize("gen", _xxhash_gens, ids=idfn)
@pytest.mark.xfail(condition = is_not_utc(), reason = 'xfail non-UTC time zone tests because of https://github.com/NVIDIA/spark-rapids/issues/9653')
def test_xxhash64_single_column(gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, gen).selectExpr("a", "xxhash64(a)"))

@ignore_order(local=True)
@pytest.mark.xfail(condition = is_not_utc(), reason = 'xfail non-UTC time zone tests because of https://github.com/NVIDIA/spark-rapids/issues/9653')
def test_xxhash64_multi_column():
    gen = StructGen(_struct_of_xxhash_gens.children, nullable=False)
    col_list = ",".join(gen.data_type.fieldNames())
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, gen).selectExpr("c0", f"xxhash64({col_list})"))

@allow_non_gpu("ProjectExec")
@ignore_order(local=True)
@pytest.mark.parametrize("gen", _xxhash_fallback_gens, ids=idfn)
def test_xxhash64_fallback(gen):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, gen).selectExpr("a", "xxhash64(a)"),
        "ProjectExec")
