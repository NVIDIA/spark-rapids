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

from data_gen import *
from spark_session import with_gpu_session

def test_uuid():
    num_rows = 2048

    # first run
    uuids_round1 = with_gpu_session(
        lambda spark: unary_op_df(spark, int_gen, length=num_rows)
            .selectExpr(["a", "uuid() as b"]).collect())

    # second run
    uuids_round2 = with_gpu_session(
        lambda spark: unary_op_df(spark, int_gen, length=num_rows).repartition(1)
            .selectExpr(["a", "uuid() as b"]).collect())

    # assert all the UUIDs of the two rounds are different
    uuid_set = set()
    for row in uuids_round1 + uuids_round2:
        uuid_set.add(row.b)
    assert len(uuid_set) == (2 * num_rows), "all the the UUIDs should be different"
