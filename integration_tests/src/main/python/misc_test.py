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

from data_gen import *
from spark_session import with_gpu_session


# GPU uuid function does not guarantee the result is the same as CPU,
# only guarantee it produces unique UUIDs
def test_uuid():
    def gen_uuids(spark):
        return unary_op_df(spark, int_gen, length=num_rows).selectExpr(["a", "uuid() as b"]).collect()

    def _verify_unique(uuids):
        uuid_set = set()
        for row in uuids:
            uuid_set.add(row.b)
        assert len(uuid_set) == len(uuids), "all the UUIDs should be different"

    num_rows = 2048

    # verify uniqueness across two rounds on GPU
    uuids_round1_gpu = with_gpu_session(lambda spark: gen_uuids(spark))
    uuids_round2_gpu = with_gpu_session(lambda spark: gen_uuids(spark))
    _verify_unique(uuids_round1_gpu + uuids_round2_gpu)

    # verify uniqueness across two rounds on CPU
    uuids_round1_cpu = with_cpu_session(lambda spark: gen_uuids(spark))
    uuids_round2_cpu = with_cpu_session(lambda spark: gen_uuids(spark))
    _verify_unique(uuids_round1_cpu + uuids_round2_cpu)
