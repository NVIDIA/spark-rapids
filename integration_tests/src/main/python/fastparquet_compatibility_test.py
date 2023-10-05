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

from asserts import assert_gpu_and_cpu_are_equal_collect
from datetime import date, datetime, timezone
from data_gen import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session

rebase_write_corrected_conf = {
    'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
    'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED'
}


def read_parquet(data_path):
    """
    (Fetches a function that) Reads Parquet from the specified `data_path`.
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
            import fastparquet
            df = fastparquet.ParquetFile(data_path).to_pandas()
            return spark.createDataFrame(df)

    return read_with_fastparquet_or_plugin


@pytest.mark.parametrize('corrected_conf', [rebase_write_corrected_conf])
@pytest.mark.parametrize('data_gen', [
    ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    pytest.param(IntegerGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    LongGen(nullable=False),
    pytest.param(LongGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    FloatGen(nullable=False),
    DoubleGen(nullable=False),
    StringGen(nullable=False),
    pytest.param(DecimalGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads Decimal columns as Float, as per "
                                                "https://fastparquet.readthedocs.io/en/latest/details.html#data-types")),
    pytest.param(DateGen(nullable=False,
                         start=date(year=2020, month=1, day=1),
                         end=date(year=2020, month=12, day=31)),
                 marks=pytest.mark.xfail(reason="fastparquet reads dates as timestamps.")),
    pytest.param(DateGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads far future dates (e.g. year=8705) incorrectly.")),
    TimestampGen(nullable=False,
                 start=datetime(2000, 1, 1, tzinfo=timezone.utc),
                 end=datetime(2200, 12, 31, tzinfo=timezone.utc)),  # Vanilla case.
    pytest.param(TimestampGen(nullable=False,
                              start=datetime(1, 1, 1, tzinfo=timezone.utc),
                              end=datetime(1899, 12, 31, tzinfo=timezone.utc)),
                 marks=pytest.mark.xfail(reason="fastparquet reads timestamps preceding 1900 incorrectly.")),
    #  TODO: Array gen type deduction borked when converting from Pandas to Spark dataframe.
    # ArrayGen(child_gen=IntegerGen(nullable=False), nullable=False),
    #  TODO: Struct rows seem to be correct, but are failing comparison because of differences in Row representation.
    # StructGen(children=[("first", IntegerGen(nullable=False)),
    #                     ("second", FloatGen(nullable=False))], nullable=False)
], ids=idfn)
def test_read_fastparquet_single_column_tables(data_gen, spark_tmp_path, corrected_conf):
    data_path = spark_tmp_path + "/FASTPARQUET_SINGLE_COLUMN_INPUT"
    gen = StructGen([('a', data_gen)], nullable=False)
    # Write data with CPU session.
    with_cpu_session(
        # Single output file, to avoid differences in order of file reads.
        lambda spark: gen_df(spark, gen, 3).repartition(1).write.mode('overwrite').parquet(data_path),
        conf=corrected_conf
    )
    # Read Parquet with CPU (fastparquet) and GPU (plugin), and compare records.
    assert_gpu_and_cpu_are_equal_collect(read_parquet(data_path), conf=corrected_conf)


@pytest.mark.parametrize('column_gen', [
    ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    pytest.param(IntegerGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    LongGen(nullable=False),
    pytest.param(LongGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    FloatGen(nullable=False),
    DoubleGen(nullable=False),
    StringGen(nullable=False),
    pytest.param(DecimalGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads Decimal columns as Float, as per "
                                                "https://fastparquet.readthedocs.io/en/latest/details.html#data-types")),
    pytest.param(DateGen(nullable=False,
                         start=date(year=2020, month=1, day=1),
                         end=date(year=2020, month=12, day=31)),
                 marks=pytest.mark.xfail(reason="fastparquet reads dates as timestamps.")),
    pytest.param(DateGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads far future dates (e.g. year=8705) incorrectly.")),
    TimestampGen(nullable=False,
                 start=datetime(2000, 1, 1, tzinfo=timezone.utc),
                 end=datetime(2200, 12, 31, tzinfo=timezone.utc)),  # Vanilla case.
    pytest.param(TimestampGen(nullable=False,
                              start=datetime(1, 1, 1, tzinfo=timezone.utc),
                              end=datetime(1899, 12, 31, tzinfo=timezone.utc)),
                 marks=pytest.mark.xfail(reason="fastparquet reads timestamps preceding 1900 incorrectly.")),
], ids=idfn)
def test_reading_file_written_with_gpu(spark_tmp_path, column_gen):
    data_path = spark_tmp_path + "/FASTPARQUET_TEST_GPU_WRITE_PATH"

    gen = StructGen([('a', column_gen),
                     ('part', IntegerGen(nullable=False))
                     ], nullable=False)
    # Write data out with Spark RAPIDS plugin.
    with_gpu_session(
        lambda spark: gen_df(spark, gen, 2048).repartition(1).write.mode('overwrite').parquet(data_path),
        conf=rebase_write_corrected_conf
    )

    # TODO: Maybe make _assert_equal() available to compare dataframes, regardless of CPU vs GPU?
    # For now, this compares the results of reading back the GPU-written data, via fastparquet and GPU.
    assert_gpu_and_cpu_are_equal_collect(read_parquet(data_path), conf=rebase_write_corrected_conf)


@pytest.mark.parametrize('column_gen', [
    ByteGen(nullable=False),
    ByteGen(nullable=True),
    ShortGen(nullable=False),
    ShortGen(nullable=True),
    IntegerGen(nullable=False),
    IntegerGen(nullable=True),
    LongGen(nullable=False),
    LongGen(nullable=True),
    FloatGen(nullable=False),
    FloatGen(nullable=True),
    DoubleGen(nullable=False),
    DoubleGen(nullable=True),
    DecimalGen(nullable=False),
    DecimalGen(nullable=True),
    pytest.param(
        StringGen(nullable=False),
        marks=pytest.mark.xfail(reason="String columns written with fastparquet are read differently between "
                                       "Apache Spark, and the Spark RAPIDS plugin. "
                                       "See <github bug link>.")),
    pytest.param(
        StringGen(nullable=True),
        marks=pytest.mark.xfail(reason="String columns written with fastparquet are read differently between "
                                       "Apache Spark, and the Spark RAPIDS plugin. "
                                       "See <github bug link>.")),
    pytest.param(
        DateGen(nullable=False,
                start=date(year=2000, month=1, day=1),
                end=date(year=2020, month=12, day=31)),
        marks=pytest.mark.xfail(reason="TODO: Test problem: Dates generated by data_gen can't be written "
                                       "with fastparquet, because the dtype/encoding cannot be deduced.")),
    pytest.param(
        TimestampGen(nullable=False),
        marks=pytest.mark.xfail(reason="Timestamps exceeding year=2300 are out of bounds for Pandas.")),
    pytest.param(
        TimestampGen(nullable=False,
                     start=datetime(2000, 1, 1, tzinfo=timezone.utc),
                     end=datetime(2200, 12, 31, tzinfo=timezone.utc)),
        marks=pytest.mark.xfail(reason="TODO: Test problem: Timestamps generated by data_gen can't be converted "
                                       "to pandas, because of type errors. ")),
    pytest.param(
        ArrayGen(IntegerGen(nullable=False), nullable=False),
        marks.pytest.mark.xfail(reason="Array columns written with fastparquet are read differently between "
                                       "Apache Spark and the Spark RAPIDS plugin. "
                                       "See <github bug link>.")),
], ids=idfn)
def test_reading_file_written_with_fastparquet(column_gen, spark_tmp_path):
    data_path = spark_tmp_path + "/FASTPARQUET_WRITE_PATH"

    def write_with_fastparquet(spark, data_gen):
        #  TODO: (future) Compression settings?
        import fastparquet
        dataframe = gen_df(spark, data_gen, 2048)
        fastparquet.write(data_path, dataframe.toPandas())

    gen = StructGen([('a', column_gen),
                     ('part', IntegerGen(nullable=False))
                     ], nullable=False)
    # Write data with CPU session.
    with_cpu_session(
        lambda spark: write_with_fastparquet(spark, gen)
    )
    # Read Parquet with CPU (Apache Spark) and GPU (plugin), and compare records.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        rebase_write_corrected_conf)
