# Copyright (c) 2026, NVIDIA CORPORATION.
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

from iceberg import get_full_table_name, iceberg_write_enabled_conf, \
    iceberg_unsupported_mark, _BASE_TBLPROPS_SQL
from marks import iceberg
from spark_session import with_gpu_session

pytestmark = iceberg_unsupported_mark


@iceberg
def test_v2_write_sql_ui_shows_gpu_child_operators(spark_tmp_table_factory):
    """Regression test: the SQL UI / History Server must show the GPU child
    operators under a DataSource V2 table write (GpuV2TableWriteExec), not just
    the write node. GpuV2TableWriteExec wraps its query in an AdaptiveSparkPlanExec
    whose own final-plan update only refreshes its subtree, so without re-posting
    the final plan the write execution's plan graph only contains the write node
    and the GPU operators under it are missing. See GpuV2TableWriteExec
    .postFinalPlanUpdateToSqlUi."""
    table_name = get_full_table_name(spark_tmp_table_factory)

    def run(spark):
        spark.sql(f"CREATE TABLE {table_name} (grp BIGINT, cnt BIGINT) "
                  f"USING ICEBERG {_BASE_TBLPROPS_SQL}")
        # GROUP BY -> shuffle -> AQE re-plans; the INSERT is the write SQL execution
        # and is the last execution, so executionsList().last() is the write.
        spark.sql(f"INSERT INTO {table_name} "
                  f"SELECT id % 8 AS grp, count(*) AS cnt FROM range(0, 100000) GROUP BY id % 8")
        # Make sure the listener bus has drained the posted plan-update events.
        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(30000)
        sql_store = spark._jsparkSession.sharedState().statusStore()
        write_exec = sql_store.executionsList().last()
        nodes = sql_store.planGraph(write_exec.executionId()).allNodes()
        names = []
        it = nodes.iterator()
        while it.hasNext():
            names.append(it.next().name())
        return names

    names = with_gpu_session(run, conf=iceberg_write_enabled_conf)

    write_markers = ("Append", "Overwrite", "Replace", "Write")
    is_write = lambda n: any(m in n for m in write_markers)
    # the write node itself must be there (sanity)
    assert any(is_write(n) for n in names), \
        f"no V2 write node in the write execution plan graph: {names}"
    # the regression: GPU child operators under the write must be present
    gpu_children = [n for n in names if n.startswith("Gpu") and not is_write(n)]
    assert gpu_children, \
        f"SQL UI plan graph for the V2 write is missing GPU child operators " \
        f"(only the write node is shown): {names}"
