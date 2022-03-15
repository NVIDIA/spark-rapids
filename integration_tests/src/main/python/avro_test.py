# Copyright (c) 2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *

pytestmark = pytest.mark.nightly_resource_consuming_test

def read_avro_df(data_path):
    return lambda spark : spark.read.format("avro").load(data_path)
'''
def read_avro_sql(data_path):
    return lambda spark : spark.sql('select * from avro.`{}`'.format(data_path))
'''
# test with original avro file reader, the multi-file parallel reader for cloud
original_avro_file_reader_conf = {'spark.rapids.sql.format.avro.reader.type': 'PERFILE'}
#multithreaded_avro_file_reader_conf = {'spark.rapids.sql.format.avro.reader.type': 'MULTITHREADED'}
#coalescing_avro_file_reader_conf = {'spark.rapids.sql.format.avro.reader.type': 'COALESCING'}
reader_opt_confs = [original_avro_file_reader_conf]

@pytest.mark.parametrize('name', ['simple.avro'])
@pytest.mark.parametrize('read_func', [read_avro_df])
@pytest.mark.parametrize('v1_enabled_list', ["avro"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_basic_read(std_input_path, name, read_func, v1_enabled_list, reader_confs):
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            read_func(std_input_path + '/' + name),
            conf=all_confs)