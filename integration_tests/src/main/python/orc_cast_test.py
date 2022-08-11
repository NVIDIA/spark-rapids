# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from pyspark.sql.types import *
from spark_session import with_cpu_session
from orc_test import reader_opt_confs

@pytest.mark.parametrize('offset', [1,2,3,4], ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_read_type_casting_integral(spark_tmp_path, offset, reader_confs, v1_enabled_list):
    int_gens = [boolean_gen] + integral_gens
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(int_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.orc(data_path))

    # build the read schema by a left shift of int_gens
    shifted_int_gens = int_gens[offset:] + int_gens[:offset]
    rs_gen_list = [('c' + str(i), gen) for i, gen in enumerate(shifted_int_gens)]
    rs = StructGen(rs_gen_list, nullable=False).data_type
    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(rs).orc(data_path),
        conf=all_confs)

@pytest.mark.parametrize('to_type', ['float', 'double', 'string', 'timestamp'])
def test_casting_from_integer(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + 'test_orc_casting'
    # Since the library 'datatime' in python, the max-year it supports is 10000, for the max value of
    # Long type, set it to '1e11'. If the long-value is out of this range, pytest will throw exception.
    data_gen = [('boolean', boolean_gen), ('tinyint', byte_gen),
                ('smallint', ShortGen(min_val=BYTE_MAX + 1)),
                ('int', IntegerGen(min_val=SHORT_MAX + 1)),
                ('bigint', LongGen(min_val=INT_MAX + 1, max_val=int(1e11))),
                ('negint', IntegerGen(max_val=-1))]
    # generate ORC dataframe, and dump it to local file
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen).write.mode('overwrite').orc(orc_path)
    )

    schema_str = "boolean {}, tinyint {}, smallint {}, int {}, bigint {}, negint {}"
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str.format(*([to_type] * 6))).orc(orc_path)
    )
