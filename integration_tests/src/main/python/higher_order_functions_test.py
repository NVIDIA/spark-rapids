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

from asserts import assert_gpu_and_cpu_are_equal_collect
from marks import ignore_order


@ignore_order(local=True)
def test_tiered_project_with_complex_transform():
    confs = {"spark.rapids.sql.tiered.project.enabled": "true"}
    def do_project(spark):
        df = spark.createDataFrame(
            [
                (1, "a", [(0, "z"), (1, "y")]),
                (2, "b", [(2, "x")])
            ],
            "a int, b string, c array<struct<x: int, y: string>>").repartition(2)
        return df.selectExpr(
            "transform(c, (v, i) -> named_struct('x', c[i].x, 'y', c[i].y)) AS t")
    assert_gpu_and_cpu_are_equal_collect(do_project, conf=confs)
