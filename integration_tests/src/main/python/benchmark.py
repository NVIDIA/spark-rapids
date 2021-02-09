# Copyright (c) 2020, NVIDIA CORPORATION.
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

import argparse
import os
import sys

def main():
    """Iterate over a series of configurations and run benchmarks for each of the specified
    queries using that configuration.

    Example usage:

    python benchmark.py \
      --template /path/to/template \
      --benchmark tpcds \
      --input /path/to/input \
      --input-format parquet \
      --output /path/to/output \
      --output-format parquet \
      --configs cpu gpu-ucx-on \
      --query q4 q5

    In this example, configuration key-value pairs will be loaded from cpu.properties and
    gpu-ucx-on.properties and appended to a spark-submit-template.txt to build the spark-submit
    commands to run the benchmark. These configuration property files simply contain key-value
    pairs in the format key=value with one pair per line. For example:

    spark.executor.cores=2
    spark.rapids.sql.enabled=true
    spark.sql.adaptive.enabled=true

    A template file must be provided, containing the command to call spark-submit along
    with any cluster-specific configuration options and any spark configuration settings that
    will be common to all benchmark runs. The template should end with a line-continuation
    symbol since additional --conf options will be appended for each benchmark run.

    Example template:

    $SPARK_HOME/bin/spark-submit \
      --master $SPARK_MASTER_URL \
      --conf spark.plugins=com.nvidia.spark.SQLPlugin \
      --conf spark.eventLog.enabled=true \
      --conf spark.eventLog.dir=./spark-event-logs \

    The output and output-format arguments can be omitted to run the benchmark and collect
    results to the driver rather than write the query output to disk.

    This benchmark script assumes that the following environment variables have been set for
    the location of the relevant JAR files to be used:

    - SPARK_RAPIDS_PLUGIN_JAR
    - SPARK_RAPIDS_PLUGIN_INTEGRATION_TEST_JAR
    - CUDF_JAR

    """

    parser = argparse.ArgumentParser(description='Run TPC benchmarks.')
    parser.add_argument('--benchmark', required=True,
                        help='Name of benchmark to run (tpcds, tpcxbb, tpch)')
    parser.add_argument('--template', required=True,
                        help='Path to a template script that invokes spark-submit')
    parser.add_argument('--input', required=True,
                    help='Path to source data set')
    parser.add_argument('--input-format', required=True,
                        help='Format of input data set (parquet or csv)')
    parser.add_argument('--append-dat', required=False, action='store_true',
                        help='Append .dat to path (for tpcds only)')
    parser.add_argument('--output', required=False,
                    help='Path to write query output to')
    parser.add_argument('--output-format', required=False,
                        help='Format to write to (parquet or orc)')
    parser.add_argument('--configs', required=True, type=str, nargs='+',
                    help='One or more configuration filenames to run')
    parser.add_argument('--query', required=True, type=str, nargs='+',
                    help='Queries to run')
    parser.add_argument('--iterations', required=False,
                        help='The number of iterations to run (defaults to 1)')
    parser.add_argument('--gc-between-runs', required=False, action='store_true',
                        help='Whether to call System.gc between iterations')
    parser.add_argument('--upload-uri', required=False,
                        help='Upload URI for summary output')

    args = parser.parse_args()

    with open(args.template, "r") as myfile:
        template = myfile.read()

    for config_name in args.configs:
        config = load_properties(config_name + ".properties")
        for query in args.query:
            summary_file_prefix = "{}-{}".format(args.benchmark, config_name)

            cmd = ['--conf spark.app.name="' + summary_file_prefix + '"']
            for k, v in config.items():
                cmd.append("--conf " + k + "=" + v)

            cmd.append("--jars $SPARK_RAPIDS_PLUGIN_JAR,$CUDF_JAR")
            cmd.append("--class com.nvidia.spark.rapids.tests.BenchmarkRunner")
            cmd.append("$SPARK_RAPIDS_PLUGIN_INTEGRATION_TEST_JAR")
            cmd.append("--benchmark " + args.benchmark)
            cmd.append("--query " + query)
            cmd.append("--input " + args.input)
            cmd.append("--input-format {}".format(args.input_format))

            if args.append_dat is True:
                cmd.append("--append-dat ")

            if args.output is not None:
                cmd.append("--output " + args.output + "/" + config_name + "/" + query)

            if args.output_format is not None:
                cmd.append("--output-format {}".format(args.output_format))

            cmd.append("--summary-file-prefix " + summary_file_prefix)

            if args.gc_between_runs is True:
                cmd.append("--gc-between-runs ")

            if args.upload_uri is not None:
                cmd.append("--upload-uri " + args.upload_uri)

            if args.iterations is None:
                cmd.append("--iterations 1")
            else:
                cmd.append("--iterations {}".format(args.iterations))

            cmd = template.strip() + "\n  " + " ".join(cmd).strip()

            # run spark-submit
            print(cmd)
            os.system(cmd)


def load_properties(filename):
    myvars = {}
    with open(filename) as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.strip()
    return myvars

if __name__ == '__main__':
    main()