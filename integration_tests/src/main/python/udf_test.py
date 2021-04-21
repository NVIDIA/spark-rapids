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

from conftest import is_at_least_precommit_run

from pyspark.sql.pandas.utils import require_minimum_pyarrow_version, require_minimum_pandas_version
try:
    require_minimum_pandas_version()
except Exception as e:
    if is_at_least_precommit_run():
        raise AssertionError("incorrect pandas version during required testing " + str(e))
    pytestmark = pytest.mark.skip(reason=str(e))

try:
    require_minimum_pyarrow_version()
except Exception as e:
    if is_at_least_precommit_run():
        raise AssertionError("incorrect pyarrow version during required testing " + str(e))
    pytestmark = pytest.mark.skip(reason=str(e))

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import incompat, approximate_float, allow_non_gpu, ignore_order
from pyspark.sql import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pandas as pd
from typing import Iterator, Tuple

arrow_udf_conf = {
    'spark.sql.execution.arrow.pyspark.enabled': 'true',
    'spark.rapids.sql.exec.WindowInPandasExec': 'true'
}

data_gens_nested_for_udf = arrow_array_gens + arrow_struct_gens

####################################################################
# NOTE: pytest does not play well with pyspark udfs, because pyspark
# tries to import the dependencies for top level functions and
# pytest messes around with imports. To make this work, all UDFs
# must either be lambdas or totally defined within the test method
# itself.
####################################################################


@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_pandas_math_udf(data_gen):
    def add(a, b):
        return a + b
    my_udf = f.pandas_udf(add, returnType=LongType())
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                my_udf(f.col('a') - 3, f.col('b'))),
            conf=arrow_udf_conf)


@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_iterator_math_udf(data_gen):
    def iterator_add(to_process: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
        for a, b in to_process:
            yield a + b

    my_udf = f.pandas_udf(iterator_add, returnType=LongType())
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                my_udf(f.col('a'), f.col('b'))),
            conf=arrow_udf_conf)


@pytest.mark.parametrize('data_gen', data_gens_nested_for_udf, ids=idfn)
def test_pandas_scalar_udf_nested_type(data_gen):
    def nested_size(nested):
        return pd.Series([nested.size]).repeat(len(nested))

    my_udf = f.pandas_udf(nested_size, returnType=LongType())
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(my_udf(f.col('a'))),
        conf=arrow_udf_conf)


@approximate_float
@allow_non_gpu('AggregateInPandasExec', 'PythonUDF', 'Alias')
@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_single_aggregate_udf(data_gen):
    @f.pandas_udf('double')
    def pandas_sum(to_process: pd.Series) -> float:
        return to_process.sum()

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                pandas_sum(f.col('a'))),
            conf=arrow_udf_conf)


@ignore_order
@allow_non_gpu('AggregateInPandasExec', 'PythonUDF', 'Alias')
@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_group_aggregate_udf(data_gen):
    @f.pandas_udf('long')
    def pandas_sum(to_process: pd.Series) -> int:
        # Sort the values before computing the sum.
        # For details please go to
        #   https://github.com/NVIDIA/spark-rapids/issues/740#issuecomment-784917512
        return to_process.sort_values().sum()

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen)\
                    .groupBy('a')\
                    .agg(pandas_sum(f.col('b'))),
            conf=arrow_udf_conf)


# ======= Test window in Pandas =======
# range frame is not supported yet.
no_part_win = Window\
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

unbounded_win = Window\
    .partitionBy('a')\
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cur_follow_win = Window\
    .partitionBy('a')\
    .orderBy('b')\
    .rowsBetween(Window.currentRow, Window.unboundedFollowing)

pre_cur_win = Window\
    .partitionBy('a')\
    .orderBy('b')\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

low_upper_win = Window.partitionBy('a').orderBy('b').rowsBetween(-3, 3)

udf_windows = [no_part_win, unbounded_win, cur_follow_win, pre_cur_win, low_upper_win]
window_ids = ['No_Partition', 'Unbounded', 'Unbounded_Following', 'Unbounded_Preceding',
              'Lower_Upper']


@ignore_order
@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
@pytest.mark.parametrize('window', udf_windows, ids=window_ids)
def test_window_aggregate_udf(data_gen, window):

    @f.pandas_udf('long')
    def pandas_sum(to_process: pd.Series) -> int:
        # Sort the values before computing the sum.
        # For details please go to
        #   https://github.com/NVIDIA/spark-rapids/issues/740#issuecomment-784917512
        return to_process.sort_values().sum()

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen).select(
            pandas_sum(f.col('b')).over(window)),
        conf=arrow_udf_conf)


@ignore_order
@pytest.mark.parametrize('data_gen', [byte_gen, short_gen, int_gen], ids=idfn)
@pytest.mark.parametrize('window', udf_windows, ids=window_ids)
def test_window_aggregate_udf_array_from_python(data_gen, window):

    @f.pandas_udf(returnType=ArrayType(LongType()))
    def pandas_sum(to_process: pd.Series) -> list:
        return [to_process.sum()]

    # When receiving the data of array type from Python side, split it right away
    # in case the following expressions or plans may not support array type yet.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen)\
            .select(pandas_sum(f.col('b')).over(window).alias('py_array'))\
            .select([f.col('py_array').getItem(i) for i in range(0, 1)]),
        conf=arrow_udf_conf)


# ======= Test flat map group in Pandas =======
@ignore_order
@pytest.mark.parametrize('data_gen', [LongGen()], ids=idfn)
def test_group_apply_udf(data_gen):
    def pandas_add(data):
        data.sum = data.b + data.a
        return data

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen)\
                    .groupBy('a')\
                    .applyInPandas(pandas_add, schema="a long, b long"),
            conf=arrow_udf_conf)


@ignore_order
@pytest.mark.parametrize('data_gen', arrow_common_gen, ids=idfn)
def test_group_apply_udf_more_types(data_gen):
    def group_size_udf(key, pdf):
        return pd.DataFrame([[len(key), len(pdf), len(pdf.columns)]])

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen, 5)\
            .groupBy('a')\
            .applyInPandas(group_size_udf, schema="c long, d long, e long"),
        conf=arrow_udf_conf)


# ======= Test map in Pandas =======
@pytest.mark.parametrize('data_gen', [LongGen()], ids=idfn)
def test_map_apply_udf(data_gen):
    def pandas_filter(iterator):
        for data in iterator:
            yield data[data.b <= data.a]

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen)\
                    .mapInPandas(pandas_filter, schema="a long, b long"),
            conf=arrow_udf_conf)


@pytest.mark.parametrize('data_gen', data_gens_nested_for_udf, ids=idfn)
def test_pandas_map_udf_nested_type(data_gen):
    # Supported UDF output types by plugin: (commonCudfTypes + ARRAY).nested() + STRUCT
    # STRUCT represents the whole dataframe in Map Pandas UDF, so no struct column in UDF output.
    # More details is here
    #   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L119
    udf_out_schema = 'c_integral long,' \
                     'c_string string,' \
                     'c_fp double,' \
                     'c_bool boolean,' \
                     'c_date date,' \
                     'c_time timestamp,' \
                     'c_array_array array<array<long>>,' \
                     'c_array_string array<string>'

    def col_types_udf(pdf_itr):
        for pdf in pdf_itr:
            # Return a data frame with columns of supported type, and there is only one row.
            # The values can not be generated randomly because it should return the same data
            # for both CPU and GPU runs.
            yield pd.DataFrame({
                "c_integral": [len(pdf)],
                "c_string": ["size" + str(len(pdf))],
                "c_fp": [float(len(pdf))],
                "c_bool": [False],
                "c_date": [date(2021, 4, 2)],
                "c_time": [datetime(2021, 4, 2, tzinfo=timezone.utc)],
                "c_array_array": [[[len(pdf)]]],
                "c_array_string": [["size" + str(len(pdf))]]
            })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen)\
            .mapInPandas(col_types_udf, schema=udf_out_schema),
        conf=arrow_udf_conf)


def create_df(spark, data_gen, left_length, right_length):
    left = binary_op_df(spark, data_gen, length=left_length)
    right = binary_op_df(spark, data_gen, length=right_length)
    return left, right


@ignore_order
@allow_non_gpu('FlatMapCoGroupsInPandasExec', 'PythonUDF', 'Alias')
@pytest.mark.parametrize('data_gen', [ShortGen(nullable=False)], ids=idfn)
def test_cogroup_apply_udf(data_gen):
    def asof_join(l, r):
        return pd.merge_asof(l, r, on='a', by='b')

    def do_it(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        return left.groupby('a').cogroup(
                right.groupby('a')).applyInPandas(
                        asof_join, schema="a int, b int")
    assert_gpu_and_cpu_are_equal_collect(do_it, conf=arrow_udf_conf)
