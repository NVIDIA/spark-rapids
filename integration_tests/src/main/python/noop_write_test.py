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
from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from data_gen import *
from marks import *
from spark_session import is_spark_330_or_later

@pytest.mark.skipif(not is_spark_330_or_later(), reason="noop format is only available in Spark 3.3.0 and later")
@pytest.mark.parametrize("mode", ["overwrite", "append", "ignore", "errorifexists"])
def test_noop_write(mode):
    def write_noop(spark, mode):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["c1", "c2"])
        df.write.format("noop").mode(mode).save()
        return df

    if mode == "overwrite":
        exist_class = "GpuOverwriteByExpressionExec"
    else:
        exist_class = "GpuAppendDataExec"

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: write_noop(spark, mode),
        exist_classes=exist_class,
        non_exist_classes="ExecutedCommandExec")
