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

from data_gen import *
from marks import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session

def create_df(spark, data_gen, left_length, right_length):
    left = binary_op_df(spark, data_gen, length=left_length)
    right = binary_op_df(spark, data_gen, length=right_length).withColumnRenamed("a", "r_a")\
            .withColumnRenamed("b", "r_b")
    return left, right


@pytest.mark.parametrize('data_gen', [StringGen()], ids=idfn)
def test_explain_join(spark_tmp_path, data_gen):
    data_path1 = spark_tmp_path + '/PARQUET_DATA1'
    data_path2 = spark_tmp_path + '/PARQUET_DATA2'

    def do_join_explain(spark):
        left, right = create_df(spark, data_gen, 500, 500)
        left.write.parquet(data_path1)
        right.write.parquet(data_path2)
        df1 = spark.read.parquet(data_path1)
        df2 = spark.read.parquet(data_path2)
        df3 = df1.join(df2, df1.a == df2.r_a, "inner")
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df3._jdf, "ALL")
        remove_isnotnull = explain_str.replace("isnotnull", "")
        # everything should be on GPU
        assert "not" not in remove_isnotnull

    with_cpu_session(do_join_explain)

def test_explain_set_config():
    conf = {'spark.rapids.sql.hasExtendedYearValues': 'false',
            'spark.rapids.sql.castStringToTimestamp.enabled': 'true'}

    def do_explain(spark):
        df = unary_op_df(spark, StringGen('[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}')).select(f.col('a').cast(TimestampType()))
        # a bit brittle if these get turned on by default
        spark.conf.set('spark.rapids.sql.hasExtendedYearValues', 'false')
        spark.conf.set('spark.rapids.sql.castStringToTimestamp.enabled', 'true')
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df._jdf, "ALL")
        print(explain_str)
        assert "timestamp) will run on GPU" in explain_str
        spark.conf.set('spark.rapids.sql.castStringToTimestamp.enabled', 'false')
        explain_str_cast_off = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df._jdf, "ALL")
        print(explain_str_cast_off)
        assert "timestamp) cannot run on GPU" in explain_str_cast_off

    with_cpu_session(do_explain)

def test_explain_udf():
    slen = udf(lambda s: len(s), IntegerType())

    @udf
    def to_upper(s):
        if s is not None:
            return s.upper()

    @udf(returnType=IntegerType())
    def add_one(x):
        if x is not None:
            return x + 1

    def do_explain(spark):
        df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
        df2 = df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age"))
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df2._jdf, "ALL")
        # udf shouldn't be on GPU
        udf_str_not = 'cannot run on GPU because GPU does not currently support the operator class org.apache.spark.sql.execution.python.BatchEvalPythonExec'
        assert udf_str_not in explain_str
        not_on_gpu_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df2._jdf, "NOT")
        assert udf_str_not in not_on_gpu_str
        assert "will run on GPU" not in not_on_gpu_str

    with_cpu_session(do_explain)


def test_explain_bucketed_scan(spark_tmp_table_factory):
    """
    https://github.com/NVIDIA/spark-rapids/issues/3952
    https://github.com/apache/spark/commit/79515e4b6c
    """
    def do_explain(spark):
        tbl_1 = spark_tmp_table_factory.get()

        spark.createDataFrame([(1, 2), (2, 3)], ("i", "j")).write.bucketBy(8, "i").saveAsTable(tbl_1)
        #spark.createDataFrame([2, 3], "i").write.bucketBy(8, "i").saveAsTable("t2")

        df1 = spark.table(tbl_1)
        #df2 = spark.table("t2")
        # join_df = df1.join(df2, df1.col("i") == df2.col("i"), "Inner")

        not_read_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df1.select(f.col("j"))._jdf, "ALL")
        print(not_read_str)
        assert "Bucketed: false (bucket column(s) not read)" in not_read_str


    with_cpu_session(do_explain)

