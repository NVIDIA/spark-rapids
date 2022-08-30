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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from pyspark.sql.types import *
from spark_session import with_cpu_session
from orc_test import reader_opt_confs


def create_orc(data_gen_list, data_path):
    # generate ORC dataframe, and dump it to local file 'data_path'
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen_list).write.orc(data_path)
    )


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('to_type', ['boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_among_integer_types(spark_tmp_path, reader_confs, v1_enabled_list, to_type):
    # cast integral types to another integral types
    int_gens = [boolean_gen] + integral_gens
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(int_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    create_orc(gen_list, data_path)

    # generate schema string like "c0 to_type, c1 to_type, ..., c4 to_type"
    schema_str = " {}, ".join([x[0] for x in gen_list]) + " {}"
    schema_str = schema_str.format(*([to_type] * len(gen_list)))
    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(data_path),
        conf=all_confs)


@pytest.mark.parametrize('to_type', ['float', 'double', 'string', 'timestamp'])
def test_casting_from_integer(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_cast_integer'
    # The Python 'datetime' module only supports a max-year of 10000, so we set the Long type max
    # to '1e11'. If the long-value is out of this range, pytest will throw an exception.
    data_gen = [('boolean_col', boolean_gen), ('tinyint_col', byte_gen),
                ('smallint_col', ShortGen(min_val=BYTE_MAX + 1)),
                ('int_col', IntegerGen(min_val=SHORT_MAX + 1)),
                ('bigint_col', LongGen(min_val=INT_MAX + 1, max_val=int(1e11))),
                ('negint_col', IntegerGen(max_val=-1))]
    create_orc(data_gen, orc_path)

    schema_str = "boolean_col {}, tinyint_col {}, smallint_col {}, int_col {}, bigint_col {}, negint_col {}"
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(
            schema_str.format(*([to_type] * len(data_gen)))).orc(orc_path)
    )

@pytest.mark.parametrize('overflow_long_gen', [LongGen(min_val=int(1e16)),
                                               LongGen(max_val=int(-1e16))])
@pytest.mark.parametrize('to_type', ['timestamp'])
def test_casting_from_overflow_long(spark_tmp_path, overflow_long_gen,to_type):
    # Timestamp(micro-seconds) is actually type of int64, when casting long(int64) to timestamp,
    # we need to multiply 1e6 (or 1e3), and it may cause overflow. This function aims to test
    # whether if 'ArithmeticException' is caught.
    orc_path = spark_tmp_path + '/orc_cast_overflow_long'
    create_orc([('long_column', overflow_long_gen)], orc_path)
    schema_str = "long_column {}".format(to_type)
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.read.schema(schema_str).orc(orc_path).collect(),
        conf={},
        error_message="ArithmeticException"
    )
