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

from conftest import skip_unless_precommit_tests


def drop_udf(spark, udf_name):
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS `{udf_name}`")


def skip_if_no_hive(spark):
    if spark.conf.get("spark.sql.catalogImplementation") != "hive":
        skip_unless_precommit_tests('The Spark session does not have Hive support')


def load_hive_udf(spark, udf_name, udf_class):
    drop_udf(spark, udf_name)
    # if UDF failed to load, throws AnalysisException, check if the udf class is in the class path
    spark.sql("CREATE TEMPORARY FUNCTION {} AS '{}'".format(udf_name, udf_class))
