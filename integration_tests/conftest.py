# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

def pytest_addoption(parser):
    """Pytest hook to define command line options for pytest"""
    parser.addoption(
        "--tpcxbb_format", action="store", default="parquet", help="format of TPCXbb data"
    )
    parser.addoption(
        "--tpcxbb_path", action="store", default=None, help="path to TPCXbb data"
    )
    parser.addoption(
        "--tpcds_format", action="store", default="parquet", help="format of TPC-DS data"
    )
    parser.addoption(
        "--tpcds_path", action="store", default=None, help="path to TPC-DS data"
    )
    parser.addoption(
        "--tpch_format", action="store", default="parquet", help="format of TPCH data"
    )
    parser.addoption(
        "--tpch_path", action="store", default=None, help="path to TPCH data"
    )
    parser.addoption(
        "--mortgage_format", action="store", default="parquet", help="format of Mortgage data"
    )
    parser.addoption(
        "--mortgage_path", action="store", default=None, help="path to Mortgage data"
    )
    parser.addoption(
        "--std_input_path", action="store", default=None, help="path to standard input files"
    )
    parser.addoption(
        "--tmp_path", action="store", default=None, help="path to store tmp files"
    )
    parser.addoption(
        "--debug_tmp_path", action='store_true', default=False, help="if true don't delete tmp_path contents for debugging"
    )
    parser.addoption(
        "--runtime_env", action='store', default="Apache", help="the runtime environment for the tests - apache or databricks"
    )
    parser.addoption(
        "--cudf_udf", action='store_true', default=False, help="if true enable cudf_udf test"
    )
    parser.addoption(
        "--rapids_udf_example_native", action='store_true', default=False,
        help="if true enable tests for RAPIDS UDF examples with native code"
    )
    parser.addoption(
        "--acceptance_tests", action='store_true', default=False,
        help="if enabled some tests that would skip if not configured correctly will instead fail"
    )

