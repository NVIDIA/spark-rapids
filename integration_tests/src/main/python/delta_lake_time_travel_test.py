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
import datetime
import logging

from asserts import *
from data_gen import *
from delta_lake_utils import delta_meta_allow
from marks import *
from spark_session import with_gpu_session
from delta.tables import *
from pyspark.sql.types import TimestampType



@delta_lake
def test_time_travel_on_non_existing_table():
    def time_travel_on_non_existing_table():
        with_gpu_session(lambda spark: spark.sql("SELECT * FROM not_existing VERSION AS OF 0"))

    assert_spark_exception(time_travel_on_non_existing_table, "AnalysisException")

def do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory, times = 2):
    table = spark_tmp_table_factory.get()
    table_path = f"{spark_tmp_path}/{table}"

    def setup_delta_table(spark):
        df = two_col_df(spark, int_gen, string_gen)
        df.write.format("delta").save(table_path)


    def append_to_delta_table(spark):
        df = two_col_df(spark, int_gen, string_gen)
        df.write.mode("append").format("delta").save(table_path)

    with_cpu_session(setup_delta_table)

    if times > 1:
        for _ in range(times - 1):
            with_cpu_session(append_to_delta_table)

    return table_path

def do_get_delta_table_timestamps(spark, table_path) -> Dict[int, datetime]:
    delta_table = DeltaTable.forPath(spark, table_path)
    commit_history_rows = delta_table.history().select("version", "timestamp").collect()
    def convert_ts(ts):
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts / 1000_000.0)
        else:
            return ts
    commit_map = {row.version: convert_ts(row.timestamp) for row in commit_history_rows}
    return commit_map


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
def test_time_travel_df_version(spark_tmp_path, spark_tmp_table_factory):
    table_path = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  times = 3)

    def check_version(spark, version):
        return spark.read.format("delta").option("versionAsOf", version).load(table_path)

    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 0))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 1))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 2))

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
def test_time_travel_sql_version(spark_tmp_path, spark_tmp_table_factory):
    table_path = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  times = 3)
    def check_version(spark, version):
        return spark.sql(f"SELECT * FROM delta.`{table_path}` VERSION AS OF {version}")

    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 0))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 1))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 2))


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
def test_time_travel_df_timestamp(spark_tmp_path, spark_tmp_table_factory):
    table_path = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  times = 3)
    commit_map = with_cpu_session(lambda spark: do_get_delta_table_timestamps(spark, table_path))
    def check_version(spark, version):
        ts = commit_map[version].isoformat()
        return spark.read.format("delta").option("timestampAsOf", ts).load(table_path)

    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 0))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 1))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 2))


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
def test_time_travel_sql_timestamp(spark_tmp_path, spark_tmp_table_factory):
    table_path = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  times = 3)
    commit_map = with_cpu_session(lambda spark: do_get_delta_table_timestamps(spark, table_path))
    def check_version(spark, version):
        ts = commit_map[version].isoformat()
        count = spark.sql(f"SELECT * FROM delta.`{table_path}` TIMESTAMP AS OF '{ts}'").count()
        logging.error(f"timestamp: {ts}, count: {count}")
        return spark.sql(f"SELECT * FROM delta.`{table_path}` TIMESTAMP AS OF '{ts}'")

    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 0))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 1))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: check_version(spark, 2))