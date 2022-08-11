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


def create_orc_dataframe(output_orc, col_name, col_gen):
    # generate ORC dataframe, and dump it to local file
    with_cpu_session(
        lambda spark: gen_df(spark, [(col_name, col_gen)]).write.mode('overwrite').orc(output_orc)
    )


# Since the library 'datatime' in python, the max-year it supports is 10000, for the max value of
# Long type, set it to '1e11'. If the long-value is out of this range, pytest will throw exception.
@pytest.mark.parametrize('from_type', [('boolean', boolean_gen), ('tinyint', byte_gen),
                                       ('smallint', ShortGen(min_val=BYTE_MAX + 1)),
                                       ('int', IntegerGen(min_val=SHORT_MAX + 1)),
                                       ('bigint', LongGen(min_val=INT_MAX + 1,
                                                          max_val=int(1e11))),
                                       ('negint', IntegerGen(max_val=-1))])
@pytest.mark.parametrize('to_type', ['float', 'double', 'string', 'timestamp'])
def test_casting_from_integer(spark_tmp_path, from_type, to_type):
    from_type, from_type_gen = from_type
    orc_path = spark_tmp_path + from_type
    create_orc_dataframe(orc_path, 'c0', from_type_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("c0 {}".format(to_type)).orc(orc_path)
    )
