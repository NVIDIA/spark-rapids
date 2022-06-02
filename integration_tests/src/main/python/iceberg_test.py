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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_before_spark_320, is_databricks_runtime, with_cpu_session

@allow_non_gpu('BatchScanExec')
@iceberg
def test_iceberg_fallback_not_unsafe_row(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        spark.sql("CREATE TABLE {} (id BIGINT, data STRING) USING ICEBERG".format(table))
        spark.sql("INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c')".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT COUNT(DISTINCT id) from {}".format(table)))

@allow_non_gpu('BatchScanExec')
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_320() or is_databricks_runtime(),
                    reason="AQE+DPP not supported until Spark 3.2.0+ and AQE+DPP not supported on Databricks")
def test_iceberg_aqe_dpp(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, int_gen, int_gen)
        df.createOrReplaceTempView("df")
        spark.sql("CREATE TABLE {} (a INT, b INT) USING ICEBERG PARTITIONED BY (a)".format(table))
        spark.sql("INSERT INTO {} SELECT * FROM df".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * from {} as X JOIN {} as Y ON X.a = Y.a WHERE Y.a > 0".format(table, table)),
        conf={"spark.sql.adaptive.enabled": "true",
              "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true"})
