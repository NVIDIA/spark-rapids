# Copyright (c) 2023, NVIDIA CORPORATION.
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
from asserts import assert_gpu_and_cpu_writes_are_equal_collect
from delta.tables import DeltaTable
from delta_lake_write_test import delta_meta_allow
from marks import allow_non_gpu, delta_lake
from pyspark.sql.functions import *
from spark_session import is_databricks104_or_later

_conf = {'spark.rapids.sql.explain': 'ALL',
         'spark.databricks.delta.autoCompact.enabled': 'true',  # Enable auto compaction.
         'spark.databricks.delta.autoCompact.minNumFiles': 3}   # Num files before compaction.


@delta_lake
@allow_non_gpu(*delta_meta_allow)
@pytest.mark.skipif(not is_databricks104_or_later(),
                    reason="Auto compaction of Delta Lake tables is only supported "
                           "on Databricks 10.4+")
def test_auto_compact(spark_tmp_path):

    data_path = spark_tmp_path + "/AUTO_COMPACT_TEST_DATA"

    # Write to Delta table. Ensure reads with CPU/GPU produce the same results.
    def write_to_delta(spark, table_path):
        input_data = spark.range(3).repartition(1)
        writer = input_data.write.format("delta").mode("append")
        writer.save(table_path)  # <-- Wait for it.
        writer.save(table_path)  # <-- Wait for it.
        writer.save(table_path)  # <-- Auto compact on 3.

    def read_data(spark, table_path):
        return spark.read.format("delta").load(table_path)

    def read_metadata(spark, table_path):
        input_table = DeltaTable.forPath(spark, table_path)
        table_history = input_table.history()
        return table_history.select(
            "version",
            "operation",
            expr("operationMetrics[\"numFiles\"]"),
            expr("operationMetrics[\"numRemovedFiles\"]"),
            expr("operationMetrics[\"numAddedFiles\"]")
        )

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func=write_to_delta,
        read_func=read_data,
        base_path=data_path,
        conf=_conf)

    assert_gpu_and_cpu_writes_are_equal_collect(
        write_func=write_to_delta,
        read_func=read_metadata,
        base_path=data_path,
        conf=_conf)
