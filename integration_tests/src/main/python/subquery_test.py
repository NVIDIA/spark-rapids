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
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_equal
from data_gen import *
from marks import *
from spark_session import with_gpu_session, with_cpu_session

gens = [('l', LongGen()), ('i', IntegerGen()), ('f', FloatGen()), (
    's', StringGen()), ('d', decimal_gen_38_10)]


@ignore_order(True)
@pytest.mark.parametrize('data_gen', [gens], ids=idfn)
def test_scalar_subquery(data_gen, spark_tmp_table_factory):
    t = spark_tmp_table_factory.get()
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen, length=1000)
            .write.format('parquet').mode('overwrite').saveAsTable(t))

    def query1(spark):
        df = spark.sql("""
        select l, i, f,
                (select count(s) from {0}) as c,
                (select count(if(i > 0, 1, null)) from {0}) as dec
        from {0}
        where l > (select max(i) from {0}) or f < (select min(i) from {0})
        """.format(t))
        df.explain(True)
        return df

    assert_gpu_and_cpu_are_equal_collect(query1)
