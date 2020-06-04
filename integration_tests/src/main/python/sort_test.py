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
from marks import *
from pyspark.sql.types import *
import pyspark.sql.functions as f

orderable_gen_classes = [ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen,
        BooleanGen, TimestampGen, DateGen, StringGen]

@pytest.mark.parametrize('data_gen_class', orderable_gen_classes, ids=idfn)
@pytest.mark.parametrize('nullable', [True, False], ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_orderby(data_gen_class, nullable, order):
    data_gen = data_gen_class(nullable=nullable)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order))

@pytest.mark.parametrize('data_gen_class', orderable_gen_classes, ids=idfn)
@pytest.mark.parametrize('nullable', [True, False], ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_sort_in_part(data_gen_class, nullable, order):
    data_gen = data_gen_class(nullable=nullable)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).sortWithinPartitions(order))

orderable_gens_sort = [byte_gen, short_gen, int_gen, long_gen,
        pytest.param(float_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/84')),
        pytest.param(double_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/84')),
        boolean_gen, timestamp_gen, date_gen, string_gen]
@pytest.mark.parametrize('data_gen', orderable_gens_sort, ids=idfn)
def test_multi_orderby(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).orderBy(f.col('a'), f.col('b').desc()))
