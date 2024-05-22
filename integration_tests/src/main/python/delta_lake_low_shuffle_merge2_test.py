# Copyright (c) 2024, NVIDIA CORPORATION.
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

import pyspark.sql.functions as f
import pytest

from delta_lake_merge_common import *
from marks import *
from pyspark.sql.types import *
from spark_session import *

delta_merge_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                           {"spark.rapids.sql.command.MergeIntoCommand": "true",
                            "spark.rapids.sql.command.MergeIntoCommandEdge": "true",
                            "spark.rapids.sql.delta.lowShuffleMerge.enabled": "true",
                            "spark.rapids.sql.format.parquet.reader.type": "PERFILE"})


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not ((is_databricks_runtime() and is_databricks133_or_later()) or
                         (not is_databricks_runtime() and spark_version().startswith("3.4"))),
                    reason="Delta Lake Low Shuffle Merge only supports Databricks 13.3 or OSS "
                           "delta 2.4")
# @pytest.mark.parametrize("table_ranges", [(range(10), range(20)),  # partial delete of target
#                                           (range(5), range(5)),  # full delete of target
#                                           (range(10), range(20, 30))  # no-op delete
#                                           ], ids=idfn)
@pytest.mark.parametrize("table_ranges", [(range(10), range(20)),  # partial delete of target
                                          ], ids=idfn)
# @pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("use_cdf", [False], ids=idfn)
# @pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None], ids=idfn)
@pytest.mark.parametrize("num_slices", [10], ids=idfn)
def test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                       use_cdf, partition_columns, num_slices):
    do_test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                          use_cdf, partition_columns, num_slices, False,
                                          delta_merge_enabled_conf)

