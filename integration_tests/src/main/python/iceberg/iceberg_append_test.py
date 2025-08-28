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
from typing import Dict, Callable, Any

import pytest

from asserts import assert_equal, assert_equal_with_local_sort
from data_gen import gen_df
from iceberg import create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name
from marks import iceberg, ignore_order
from spark_session import with_gpu_session, with_cpu_session


def do_test_insert_into_table_sql(spark_tmp_table_factory,
                                  create_table_func: Callable[[str], Any]):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf = {"spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                             "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED"})
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf = {"spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                             "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED"})

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_unpartitioned_table_all_cols(spark_tmp_table_factory, format_version, write_distribution_mode):
    table_prop = {"format-version": format_version,
                  "write.distribution-mode": write_distribution_mode}

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("fanout", [True, False], ids=lambda x: f"fanout={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_insert_into_partitioned_table_all_cols(spark_tmp_table_factory, format_version, fanout, write_distribution_mode):
    table_prop = {"format-version": format_version,
                   "write.spark.fanout.enabled": str(fanout).lower(),
                  "write.distribution-mode": write_distribution_mode}

    def create_table_and_set_write_order(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
            table_prop=table_prop)

        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY _c2, _c3, _c4"
        with_cpu_session(lambda spark: spark.sql(sql))
        return table_name



    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        create_table_and_set_write_order)