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
import tempfile

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from iceberg import iceberg_gens_list, iceberg_base_table_cols, rapids_reader_types, \
    eq_column_combinations, setup_base_iceberg_table, _add_eq_deletes, _change_table
from marks import iceberg, ignore_order, inject_oom
from spark_session import is_spark_35x, with_gpu_session

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Current spark-rapids only support spark 3.5.x")


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('eq_delete_cols',
                         eq_column_combinations(iceberg_base_table_cols, iceberg_gens_list),
                         ids=lambda x: str(x))
def test_iceberg_v2_eq_deletes(spark_tmp_table_factory, spark_tmp_path, reader_type,
                               eq_delete_cols):
    table_name = setup_base_iceberg_table(spark_tmp_table_factory)

    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, list(eq_delete_cols), 120, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v2_position_delete(spark_tmp_table_factory, reader_type):
    # We use a fixed seed here to ensure that data deletion vector has been generated
    table_name = setup_base_iceberg_table(spark_tmp_table_factory,
                                          seed=1743493804)
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v2_position_delete_with_url_encoded_path(spark_tmp_table_factory,
                                                          spark_tmp_path,
                                                          reader_type):
    # We use a fixed seed here to ensure that data deletion vector has been generated
    temp_dir = tempfile.mkdtemp(dir=spark_tmp_path)
    data_path = f'{temp_dir}/tb=%2F%23_v9kRtI%27/data'
    table_name = setup_base_iceberg_table(spark_tmp_table_factory,
                                          seed=1743493804,
                                          table_prop={'write.data.path': data_path})
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v2_mixed_deletes(spark_tmp_table_factory, spark_tmp_path, reader_type):
    # We use a fixed seed here to ensure that data deletion vector has been generated
    table_name = setup_base_iceberg_table(spark_tmp_table_factory,
                                          seed=1743493804)
    # Equation deletes
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c0"], 170, table_name, spark_tmp_path),
                  "No equation deletes generated")

    # Position deletes
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")

    # Equation deletes
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c1", "_c2"], 110, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    # Equation deletes
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c2", "_c3", "_c6"], 140, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    # Trigger a count operation to verify that it works
    with_gpu_session(lambda spark: spark.table(table_name).count(),
                     conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


