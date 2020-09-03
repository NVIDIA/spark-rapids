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


from pyspark import SparkConf, SparkContext, SQLContext
from asserts import assert_gpu_and_cpu_are_equal_collect
import pytest
from marks import allow_non_gpu, ignore_order, udf
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType


@pandas_udf('int', PandasUDFType.GROUPED_AGG)
def min_udf(v):
    return v.min()

@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def normalize(pdf):
    v = pdf.v
    return pdf.assign(v=(v - v.mean()) / v.std())

@pandas_udf("integer")
def add_one(s):
    return s + 1

@pandas_udf("integer")  
def sum_udf(v: pd.Series) -> int:
    return v.sum()

def _create_df(spark):
    return spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
            ("id", "v")
        )

def _with_column(spark):
    df = _create_df(spark)
    return df.withColumn('v1', add_one(df.v))

def _group_agg(spark):
    df = _create_df(spark)
    return df.groupBy(df.id).agg(min_udf(df.v))

def _group_apply(spark):
    df = _create_df(spark)
    return df.groupBy(df.id).apply(normalize)

def _group_apply_in_pandas(spark):
    df = _create_df(spark)
    def _normalize(pdf):
        v = pdf.v
        return pdf.assign(v=(v - v.mean()) / v.std())
    return df.groupBy(df.id).applyInPandas(_normalize, schema=df.schema)

def _cogroup_apply_in_pandas(spark):
    df1 = spark.createDataFrame(
            [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
            ("time", "id", "v1"))

    df2 = spark.createDataFrame(
            [(20000101, 1, "x"), (20000101, 2, "y")],
            ("time", "id", "v2"))
    def asof_join(l, r):
        return pd.merge_asof(l, r, on="time", by="id")
    return df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
            asof_join, schema="time int, id int, v1 double, v2 string")

def _sql_udf(spark):
    _ = spark.udf.register("add_one", add_one)
    return spark.sql("SELECT add_one(id) FROM range(3)")

def _sql_group_udf(spark):
    _ = spark.udf.register("sum_udf", sum_udf)
    q = "SELECT sum_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
    return spark.sql(q)

def _select_udf(spark):
    df = _create_df(spark)
    return df.select(add_one(df.v))

def _map_in_pandas(spark):
    df = _create_df(spark)
    def filter_func(iterator):
        for pdf in iterator:
            yield pdf[pdf.id == 1]
    return df.mapInPandas(filter_func, df.schema)

_conf = {
        'spark.rapids.sql.exec.ArrowEvalPythonExec':'true',
        'spark.rapids.sql.exec.MapInPandasExec':'true',
        'spark.rapids.sql.exec.FlatMapGroupsInPandasExec': 'true',
        'spark.rapids.sql.exec.AggregateInPandasExec': 'true',
        'spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec': 'true'
        }

ARROW_EVAL_PYTHON_EXEC = [_sql_udf, _select_udf, _with_column]
FLAT_MAP_GROUPS_IN_PANDAS_EXEC = [_group_apply, _group_apply_in_pandas]
AGGREGATE_IN_PANDAS_EXEC = [_group_agg, _sql_group_udf]
MAP_IN_PANDAS_EXEC = [_map_in_pandas]
FLAT_MAP_CO_CROUPS_IN_PANDAS_EXEC = [_cogroup_apply_in_pandas]


@allow_non_gpu('GpuArrowEvalPythonExec,PythonUDF')
@pytest.mark.parametrize('func', ARROW_EVAL_PYTHON_EXEC)
@udf
def test_arrow_eval(func):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: func(spark), conf=_conf)


@allow_non_gpu('GpuFlatMapGroupsInPandasExec,PythonUDF')
@pytest.mark.parametrize('func', FLAT_MAP_GROUPS_IN_PANDAS_EXEC)
@ignore_order
@udf
def test_flat_map_groups_in_pandas(func):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: func(spark), conf=_conf)

#@allow_non_gpu('GpuAggregateInPandasExec,PythonUDF,LocalTableScanExec,AggregateInPandasExec')
@allow_non_gpu(any=True)
@pytest.mark.parametrize('func', AGGREGATE_IN_PANDAS_EXEC)
@ignore_order
@udf
def test_agg_in_pandas(func):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: func(spark), conf=_conf)

@allow_non_gpu('GpuMapInPandasExec,PythonUDF')
@pytest.mark.parametrize('func', MAP_IN_PANDAS_EXEC)
@udf
def test_map_in_pandas(func):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: func(spark), conf=_conf)

@allow_non_gpu('GpuFlatMapCoGroupsInPandasExec,PythonUDF')
@pytest.mark.parametrize('func', FLAT_MAP_CO_CROUPS_IN_PANDAS_EXEC)
@ignore_order
@udf
def test_flat_map_cogroup_apply_in_pandas(func):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: func(spark), conf=_conf)


