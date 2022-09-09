# Copyright (c) 2022, NVIDIA CORPORATION.
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
from pyspark.sql.functions import when, col
from pyspark.sql.types import *
from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import ignore_order, allow_non_gpu
from spark_session import with_cpu_session

_adaptive_conf = { "spark.sql.adaptive.enabled": "true" }

def create_skew_df(spark, length):
    root = spark.range(0, length)
    mid = length / 2
    left = root.select(
        when(col('id') < mid / 2, mid).
            otherwise('id').alias("key1"),
        col('id').alias("value1")
    )
    right = root.select(
        when(col('id') < mid, mid).
            otherwise('id').alias("key2"),
        col('id').alias("value2")
    )
    return left, right


# This replicates the skew join test from scala tests, and is here to test
# the computeStats(...) implementation in GpuRangeExec
@ignore_order(local=True)
def test_aqe_skew_join():
    def do_join(spark):
        left, right = create_skew_df(spark, 500)
        left.createOrReplaceTempView("skewData1")
        right.createOrReplaceTempView("skewData2")
        return spark.sql("SELECT * FROM skewData1 join skewData2 ON key1 = key2")

    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_adaptive_conf)

# Test the computeStats(...) implementation in GpuDataSourceScanExec
@ignore_order(local=True)
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_join_parquet(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).orderBy('a').write.parquet(data_path)
    )

    def do_it(spark):
        spark.read.parquet(data_path).createOrReplaceTempView('df1')
        spark.read.parquet(data_path).createOrReplaceTempView('df2')
        return spark.sql("select count(*) from df1,df2 where df1.a = df2.a")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=_adaptive_conf)


# Test the computeStats(...) implementation in GpuBatchScanExec
@ignore_order(local=True)
@pytest.mark.parametrize("data_gen", integral_gens, ids=idfn)
def test_aqe_join_parquet_batch(spark_tmp_path, data_gen):
    # force v2 source for parquet to use BatchScanExec
    conf = copy_and_update(_adaptive_conf, {
        "spark.sql.sources.useV1SourceList": ""
    })

    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, data_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'

    def do_it(spark):
        spark.read.parquet(data_path).createOrReplaceTempView('df1')
        spark.read.parquet(data_path).createOrReplaceTempView('df2')
        return spark.sql("select count(*) from df1,df2 where df1.a = df2.a")

    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)

# Test the map stage submission handling for GpuShuffleExchangeExec
@ignore_order(local=True)
def test_aqe_struct_self_join(spark_tmp_table_factory):
    def do_join(spark):
        data = [
            (("Adam ", "", "Green"), "1", "M", 1000),
            (("Bob ", "Middle", "Green"), "2", "M", 2000),
            (("Cathy ", "", "Green"), "3", "F", 3000)
        ]
        schema = (StructType()
                  .add("name", StructType()
                       .add("firstname", StringType())
                       .add("middlename", StringType())
                       .add("lastname", StringType()))
                  .add("id", StringType())
                  .add("gender", StringType())
                  .add("salary", IntegerType()))
        df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        df_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(df_name)
        resultdf = spark.sql(
            "select struct(name, struct(name.firstname, name.lastname) as newname)" +
            " as col,name from " + df_name + " union" +
            " select struct(name, struct(name.firstname, name.lastname) as newname) as col,name" +
            " from " + df_name)
        resultdf_name = spark_tmp_table_factory.get()
        resultdf.createOrReplaceTempView(resultdf_name)
        return spark.sql("select a.* from {} a, {} b where a.name=b.name".format(
            resultdf_name, resultdf_name))
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_adaptive_conf)
