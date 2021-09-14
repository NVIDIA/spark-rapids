# Copyright (c) 2021, NVIDIA CORPORATION.
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
from asserts import assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import *

gens = [('l', LongGen()), ('i', IntegerGen()), ('f', FloatGen()), (
    's', StringGen())]


@ignore_order
@pytest.mark.parametrize('data_gen', [gens], ids=idfn)
def test_scalar_subquery(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        'table',
        '''
        select l, i, f, (select count(s) from table) as c
        from table
        where l > (select max(i) from table) or f < (select min(i) from table)
        ''')
