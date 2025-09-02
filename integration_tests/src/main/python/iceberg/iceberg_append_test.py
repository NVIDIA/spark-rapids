# Copyright (c) 2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_sql_fallback_collect
from marks import iceberg, allow_non_gpu
from spark_session import is_spark_35x, with_cpu_session

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Current spark-rapids only support spark 3.5.x")


@iceberg
@allow_non_gpu("AppendDataExec")
def test_insert_into_iceberg_table_fallback(spark_tmp_table_factory):
    table_name = spark_tmp_table_factory.get()

    def create_iceberg_table(spark):
        spark.sql(f"""CREATE TABLE {table_name} (
                id INT,
                data STRING
            ) USING ICEBERG
        """)
    with_cpu_session(create_iceberg_table)

    def df_fun(spark):
        return spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'data'])

    view_name = spark_tmp_table_factory.get()
    assert_gpu_sql_fallback_collect(
        df_fun,
        'AppendDataExecxx',
        view_name,
        f"INSERT INTO {table_name} SELECT * FROM {view_name}")
