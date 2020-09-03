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
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from spark_session import with_cpu_session, with_gpu_session
from marks import udf, allow_non_gpu

_conf = {
        'spark.rapids.sql.exec.ArrowEvalPythonExec':'true',
        'spark.rapids.sql.exec.MapInPandasExec':'true',
        'spark.rapids.sql.exec.FlatMapGroupsInPandasExec': 'true',
        'spark.rapids.sql.exec.AggregateInPandasExec': 'true',
        'spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec': 'true'
        }

@pandas_udf('int')
def _plus_one_cpu_func(v: pd.Series) -> pd.Series:
    return v + 1


@pandas_udf('int')
def _plus_one_gpu_func(v: pd.Series) -> pd.Series:
    import cudf
    gpu_serises = cudf.Series(v)
    gpu_serises = gpu_serises + 1
    return gpu_serises.to_pandas()


@allow_non_gpu(any=True)
@udf
def test_with_column():
    def cpu_run(spark):
        df = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
            ("id", "v")
        )
        return df.withColumn("v1", _plus_one_cpu_func(df.v)).collect()

    def gpu_run(spark):
        df = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
            ("id", "v")
        )
        return df.withColumn("v1", _plus_one_gpu_func(df.v)).collect()
    cpu_ret = with_cpu_session(cpu_run, conf=_conf)
    gpu_ret = with_gpu_session(gpu_run, conf=_conf)
    print(cpu_ret)
    print(gpu_ret)
    assert cpu_ret == gpu_ret

#To solve: Invalid udf: the udf argument must be a pandas_udf of type GROUPED_MAP
#need to add udf type
@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def _sum_cpu_func(df):
    v = df.v
    return df.assign(v=(v - v.mean()) / v.std())

@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def _sum_gpu_func(df):
    import cudf
    gdf = cudf.from_pandas(df)
    v = gdf.v
    return gdf.assign(v=(v - v.mean()) / v.std()).to_pandas()

@allow_non_gpu(any=True)
@udf
def test_group_apply():
    def cpu_run(spark):
        df = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
            ("id", "v")
        )
        return df.groupby("id").apply(_sum_cpu_func).collect()

    def gpu_run(spark):
        df = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
            ("id", "v")
        )
        return df.groupby("id").apply(_sum_gpu_func).collect()
    cpu_ret = with_cpu_session(cpu_run, conf=_conf)
    gpu_ret = with_gpu_session(gpu_run, conf=_conf)
    print(cpu_ret)
    print(gpu_ret)
    assert cpu_ret.sort() == gpu_ret.sort()

