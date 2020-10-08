import argparse
import os
import sys

def main():
    """Iterate over a series of configurations and run benchmarks for each of the specified
    queries using that configuration.

    Example usage:

    python benchmark.py
      --benchmark tpcds \
      --input /path/to/input \
      --input-format parquet \
      --output /path/to/output \
      --output-format parquet \
      --configs cpu gpu-ucx-on
      --query q4 q5

    In this example, configuration key-value pairs will be loaded from cpu.properties and
    gpu-ucx-on.properties and appended to a spark-submit-template.txt to build the spark-submit
    commands to run the benchmark.

    The spark-submit-template.txt file should contain the command to call spark-submit along
    with any cluster-specific configuration options and any spark configuration settings that
    will be common to all benchmark runs. The template should end with a line-continuation
    symbol since additional --conf options will be appended for each benchmark run.

    Example spark-submit-template.txt:

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
    - $SPARK_RAPIDS_PLUGIN_INTEGRATION_TEST_JAR
    - $CUDF_JAR

    """

    parser = argparse.ArgumentParser(description='Run TPC benchmarks.')
    parser.add_argument('--benchmark',
                        help='Name of benchmark to run (tpcds, tpcxbb, tpch)')
    parser.add_argument('--input',
                    help='Path to source data set')
    parser.add_argument('--input-format',
                        help='Format of input data set (parquet or csv)')
    parser.add_argument('--output',
                    help='Path to write query output to')
    parser.add_argument('--output-format',
                        help='Format to write to (parquet or orc)')
    parser.add_argument('--configs', type=str, nargs='+',
                    help='One or more configuration filenames to run')
    parser.add_argument('--query', type=str, nargs='+',
                    help='Queries to run')
    parser.add_argument('--iterations',
                        help='The number of iterations to run (defaults to 1)')

    args = parser.parse_args()

    if args.benchmark == "tpcds":
        class_name = "com.nvidia.spark.rapids.tests.tpcds.TpcdsLikeBench"
    elif args.benchmark == "tpcxbb":
        class_name = "com.nvidia.spark.rapids.tests.tpcxbb.TpcxbbLikeBench"
    elif args.benchmark == "tpch":
        class_name = "com.nvidia.spark.rapids.tests.tpch.TpchLikeBench"
    else:
        sys.exit("invalid benchmark name")

    spark_submit_template = "spark-submit-template.txt"

    with open(spark_submit_template, "r") as myfile:
        template = myfile.read()

    for config_name in args.configs:
        config = load_properties(config_name + ".properties")
        for query in args.query:
            summary_file_prefix = "{}-{}-{}".format(args.benchmark, query, config_name)

            cmd = ['--conf spark.app.name="' + summary_file_prefix + '"']
            for k, v in config.items():
                cmd.append("--conf " + k + "=" + v)

            cmd.append("--jars $SPARK_RAPIDS_PLUGIN_JAR,$CUDF_JAR")
            cmd.append("--class " + class_name)
            cmd.append("$SPARK_RAPIDS_PLUGIN_INTEGRATION_TEST_JAR")
            cmd.append("--input " + args.input)
            
            if args.input_format is not None:
                cmd.append("--input-format {}".format(args.input_format))

            if args.output is not None:
                cmd.append("--output " + args.output + "/" + config_name + "/" + query)

            if args.output_format is not None:
                cmd.append("--output-format {}".format(args.output_format))

            cmd.append("--query " + query)
            cmd.append("--summary-file-prefix " + summary_file_prefix)

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