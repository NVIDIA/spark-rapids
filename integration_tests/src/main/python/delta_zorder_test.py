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
from marks import allow_non_gpu, ignore_order, delta_lake
from spark_session import is_databricks_runtime, with_cpu_session, with_gpu_session

# Almost all of this is the metadata query
# the important part is to not have InterleaveBits or PartitionerExpr
@allow_non_gpu("SerializeFromObjectExec", "MapElementsExec", "MapPartitionsExec", "DeserializeToObjectExec",
        "ProjectExec", "FilterExec", "SortExec", "ShuffleExchangeExec", "FileSourceScanExec",
        "ObjectHashAggregateExec", "CollectLimitExec")
@delta_lake
@pytest.mark.skipif(is_databricks_runtime(), reason='We do not support zorder for databricks yet')
@ignore_order(local=True)
def test_delta_zorder(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()
    def setup_delta_table(spark):
        df = two_col_df(spark, long_gen, string_gen, length=4096)
        spark.sql("DROP TABLE IF EXISTS {}".format(table)).show()
        spark.sql("CREATE TABLE {} (a BIGINT, b STRING) USING DELTA".format(table)).show()
        df.write.insertInto(table)

    with_cpu_session(setup_delta_table)

    def optimize_table(spark):
        # The optimize returns stats and metadata about the operation, which is different
        # from one run to another, so we cannot just compare them...
        spark.sql("OPTIMIZE {} ZORDER BY a, b".format(table)).show()
        return spark.sql("select * from {} where a = 1".format(table))

    assert_gpu_and_cpu_are_equal_collect(optimize_table,
            conf={"spark.rapids.sql.castFloatToIntegralTypes.enabled": True,
                  "spark.rapids.sql.castFloatToString.enabled": True,
                  "spark.rapids.sql.explain": "ALL"})

