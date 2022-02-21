# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
from asserts import assert_gpu_and_cpu_are_equal_sql, _RowCmp, assert_equal
from data_gen import *
from marks import *
from spark_session import with_cpu_session, with_gpu_session

gens = [('l', LongGen()), ('l2', LongGen()),
        ('i', IntegerGen()), ('i2', IntegerGen()),  ('i3', IntegerGen()),
        ('f', FloatGen()),
        ('s', StringGen()),
        ('d', decimal_gen_38_10)]

@ignore_order
@pytest.mark.parametrize('data_gen', [gens], ids=idfn)
def test_scalar_subquery(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        'table',
        '''
        select l, i, f, (select count(s) from table) as c, (select max(d) from table) as dec
        from table
        where l > (select max(i) from table) or f < (select min(i) from table)
        ''')

@ignore_order(True)
@approximate_float
@pytest.mark.parametrize('q_index', range(3, 4), ids=idfn)
@pytest.mark.parametrize('data_gen', [gens], ids=idfn)
def test_combine_aggregate_for_scalar_subquery(q_index, data_gen, spark_tmp_table_factory):
    queries = [
        """
            select l, i, f,
                    (select count(s) from {0}) as c,
                    (select count(if(i > 0, 1, null)) from {0}) as dec
            from {0}
            where l > (select max(i) from {0}) or f < (select min(i) from {0})
        """,
        """
            select l, i, f,
                    (select count(s) from {0}) as cnt,
                    (select avg(i) from {0}) as avg_i,
                    (select max(l) from {0} where i > 0) as max_l,
                    (select min(s) from {0} where i > 0) as min_s
            from {0}
        """,
        """
            select sum(i) from {0}
            where l > (select min(l + l2 + i) from {0} where l > 0)
                and l2 < (select max(i) + max(i2) from {0} where l > 0)
                and i2 > (select count(if(i % 2 == 0, 1, null)) from {0} where l < 0)
                and i > (select count(if(i2 % 2 == 0, 1, null)) from {0} where l < 0)
        """,
        """
            select l, i, f,
                    (select sum(coalesce(i + i2, 1) % 100) from {0} where l != 0) as sum_is,
                    (select min(i + i3) from {0} where l != 0) as min_is,
                    (select max(l + l2) from {0} where i < 0) as max_l,
                    (select min(i2 + i3) from {0} where i < 0) as sum_i,
                    (select max(mod10) from (select l % 10 as mod10 from {0} where i < 0)) as max_mod10,
                    (select min(mod10) from (select l % 10 as mod10 from {0} where i < 0)) as min_mod10
            from {0}
        """
    ]

    t = spark_tmp_table_factory.get()
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen)
            .write.format('parquet').mode('overwrite').saveAsTable(t))

    def query_exec(spark):
        return spark.sql(queries[q_index].format(t))

    cpu_ret = with_cpu_session(
        lambda spark: query_exec(spark).collect())
    gpu_comb_ret = with_gpu_session(
        lambda spark: query_exec(spark).collect(),
        conf={'spark.rapids.sql.combineAggregate.enabled': 'true'})
    cpu_comb_ret = with_cpu_session(
        lambda spark: query_exec(spark).collect(),
        conf={'spark.rapids.sql.combineAggregate.enabled': 'true'})
    cpu_ret.sort(key=_RowCmp)
    cpu_comb_ret.sort(key=_RowCmp)
    gpu_comb_ret.sort(key=_RowCmp)
    assert_equal(cpu_ret, cpu_comb_ret)
    assert_equal(cpu_ret, gpu_comb_ret)
