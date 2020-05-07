# RAPIDS Plugin For Apache Spark Integration Tests

This is a set of integration tests for the RAPIDS Plugin for Apache Spark. These tests
are intended to be able to be run against any Spark-compatible cluster/release to help
verify that the plugin is doing the right thing in as many cases as possible.

## Dependencies

The tests are based off of `pyspark` and `pytest` running on Python 3. There really are
only a small number of Python dependencies that you need to install for the tests. The
dependencies also only need to be on the driver.  You can install them on all nodes
in the cluster but it is not required.

### pytest
`pip install pytest`

Should be enough to get the basics started.

### sre_yield
`pip install sre_yield`

`sre_yield` provides a set of APIs to generate string data from a regular expression.

## Running

Running the tests follows the pytest conventions, the main difference is using
`spark-submit` to launch the tests instead of pytest.

```$SPARK_HOME/bin/spark-submit ./runtests.py```

See `pytest -h` or `$SPARK_HOME/bin/spark-submit ./runtests.py -h` for more options.

Most clusters probably will not have the RAPIDS plugin installed in the cluster yet.
If just want to verify the SQL replacement is working you will need to add the `rapids-4-spark` and `cudf` jars to your `spark-submit` command.

```$SPARK_HOME/bin/spark-submit --jars "../dist/target/rapids-4-spark_2.12-0.1-SNAPSHOT.jar,$CUDF/java/target/cudf-0.14-SNAPSHOT.jar" ./runtests.py```

You don't have to enable the plugin for this to work, the test framework will do that for you.

All of the tests will run in a single application.  They just enable and disable the plugin as needed.

You do need to have access to a compatible GPU with the needed CUDA drivers. The exact details of how to set this up are beyond the scope of this document, but the Spark feature for scheduling GPUs does make this very simple if you have it configured.

### timezone

The RAPIDS plugin currently only supports the UTC time zone. Spark uses the default system time zone unless explicitly set otherwise.
To make sure that the tests work properly you need to configure your cluster or application to run with UTC.
The python framework cannot always do this for you because it risks overwriting other java options in the config.
Please be sure that the following configs are set when running the tests.

  * `spark.driver.extraJavaOptions` should include `-Duser.timezone=GMT`
  * `spark.executor.extraJavaOptions` should include `-Duser.timezone=GMT`
  * `spark.sql.session.timeZone`=`UTC`

### Enabling TPCxBB/TPCH/Mortgage Tests

The TPCxBB, TPCH, and Mortgage tests in this framework can be enabled by providing a couple of options:

   * TPCxBB `tpcxbb-format` (optional, defaults to "parquet"), and `tpcxbb-path` (required, path to the TPCxBB data).
   * TPCH `tpch-format` (optional, defaults to "parquet"), and `tpch-path` (required, path to the TPCH data).
   * Mortgage `mortgage-format` (optional, defaults to "parquet"), and `mortgage-path` (required, path to the Mortgage data).

As an example, here is the `spark-submit` command with the TPCxBB parameters:

```$SPARK_HOME/bin/spark-submit --jars "../dist/target/rapids-4-spark_2.12-0.1-SNAPSHOT.jar,$CUDF/java/target/cudf-0.14-SNAPSHOT.jar,../tests/target/rapids-4-spark-tests_2.12-0.1-SNAPSHOT.jar" ./runtests.py --tpcxbb_format="csv" --tpcxbb_path="/path/to/tpcxbb/csv"```


