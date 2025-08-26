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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import gen_df
from iceberg import create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, \
    rapids_reader_types
from marks import iceberg, ignore_order
from spark_session import with_gpu_session


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_insert_into_unpartitioned_table_all_cols(spark_tmp_table_factory, reader_type):
    table_name = create_iceberg_table(spark_tmp_table_factory)

    def insert_data(spark):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(insert_data)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

