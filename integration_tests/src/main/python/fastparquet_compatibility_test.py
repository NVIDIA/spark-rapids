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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_fallback_collect, assert_gpu_sql_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from spark_session import is_before_spark_320, is_databricks113_or_later, spark_version, with_cpu_session
import warnings
import fastparquet as fastpak


def read_parquet(data_path):
    """
    (Fetches a function that) reads Parquet from the specified `data_path`.
    If the plugin is enabled, the read is done via Spark APIs, through the plugin.
    If the plugin is disabled, the data is read via `fastparquet`.
    :param data_path: Location of the (single) Parquet input file.
    :return: A function that reads Parquet, via the plugin or `fastparquet`.
    """
    def read_with_fastparquet_or_plugin(spark):
        plugin_enabled = spark.conf.get("spark.rapids.sql.enabled", "false") == "true"
        if plugin_enabled:
            return spark.read.parquet(data_path)
        else:
            df = fastpak.ParquetFile(data_path).to_pandas()
            return spark.createDataFrame(df)
    return read_with_fastparquet_or_plugin


@pytest.mark.parametrize('data_gen', [
    ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    # pytest.param(IntegerGen(nullable=True),
    #              marks=pytest.mark.xfail(reason="Nullable Integers are promoted to bigint by fastparquet")),
    LongGen(nullable=False),
    # pytest.param(LongGen(nullable=True),
    #              marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    FloatGen(nullable=False),
    DoubleGen(nullable=False),
    StringGen(nullable=False),
], ids=idfn)
def test_read_fastparquet_single_column_tables(data_gen, spark_tmp_path):
    data_path = spark_tmp_path + "/FASTPARQUET_SINGLE_COLUMN_INPUT"
    gen = StructGen([('a', data_gen)], nullable=False)
    # Write data with CPU session.
    with_cpu_session(
        lambda spark: gen_df(spark, gen, 100).repartition(1).write.mode('overwrite').parquet(data_path)
    )
    # Read Parquet with CPU (fastparquet) and GPU (plugin), and compare records.
    assert_gpu_and_cpu_are_equal_collect(read_parquet(data_path))

