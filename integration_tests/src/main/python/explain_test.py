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
from spark_session import with_cpu_session

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
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGPUPlan(df3._jdf, "ALL")
        remove_isnotnull = explain_str.replace("isnotnull", "")
        # everything should be on GPU
        assert "not" not in remove_isnotnull

    with_cpu_session(do_join_explain)


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

    def do_join_explain(spark):
        df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
        df2 = df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age"))
        explain_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGPUPlan(df2._jdf, "ALL")
        # udf shouldn't be on GPU
        udf_str_not = 'cannot run on GPU because no GPU enabled version of operator class org.apache.spark.sql.execution.python.BatchEvalPythonExec'
        assert udf_str_not in explain_str
        not_on_gpu_str = spark.sparkContext._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGPUPlan(df2._jdf, "NOT")
        assert udf_str_not in not_on_gpu_str
        assert "will run on GPU" not in not_on_gpu_str

    with_cpu_session(do_join_explain)

