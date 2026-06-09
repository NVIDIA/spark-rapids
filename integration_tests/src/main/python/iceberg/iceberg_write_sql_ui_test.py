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

from iceberg import get_full_table_name, iceberg_write_enabled_conf, \
    iceberg_unsupported_mark, _BASE_TBLPROPS_SQL
from marks import allow_non_gpu, iceberg
from spark_session import with_gpu_session

pytestmark = iceberg_unsupported_mark

# A V2 write node (e.g. GpuAppendData) shows up in the write execution's plan graph
# with one of these markers in its node name.
_WRITE_MARKERS = ("Append", "Overwrite", "Replace", "Write")


def _is_write_node(name):
    return any(m in name for m in _WRITE_MARKERS)


# CREATE TABLE ... USING ICEBERG is a CPU-only catalog op (CreateTableExec) that is
# not (and need not be) on the GPU. In GPU test mode the plugin asserts the full plan
# is columnar unless the CPU node is allowed, so allow it here; the INSERT below is the
# write execution we actually assert on.
@allow_non_gpu('CreateTableExec')
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
        # GROUP BY -> shuffle -> AQE re-plans; the INSERT is the V2 write execution.
        spark.sql(f"INSERT INTO {table_name} "
                  f"SELECT id % 8 AS grp, count(*) AS cnt FROM range(0, 100000) GROUP BY id % 8")
        # Make sure the listener bus has drained the posted plan-update events.
        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(30000)
        sql_store = spark._jsparkSession.sharedState().statusStore()

        def node_names(exec_id):
            names = []
            it = sql_store.planGraph(exec_id).allNodes().iterator()
            while it.hasNext():
                names.append(it.next().name())
            return names

        # Pick the write execution by plan content rather than list position:
        # executionsList() ordering is implementation-defined and varies across Spark
        # releases, and the CREATE TABLE above is its own SQL execution. The write
        # execution is the one whose plan graph is rooted at / contains a write node.
        execs = sql_store.executionsList()
        it = execs.iterator()
        while it.hasNext():
            names = node_names(it.next().executionId())
            if any(_is_write_node(n) for n in names):
                return names
        return []

    names = with_gpu_session(run, conf=iceberg_write_enabled_conf)

    # the write node itself must be there (sanity / confirms we found the write execution)
    assert any(_is_write_node(n) for n in names), \
        f"no V2 write node found in any SQL execution's plan graph: {names}"
    # the regression: GPU child operators under the write must be present
    gpu_children = [n for n in names if n.startswith("Gpu") and not _is_write_node(n)]
    assert gpu_children, \
        f"SQL UI plan graph for the V2 write is missing GPU child operators " \
        f"(only the write node is shown): {names}"
