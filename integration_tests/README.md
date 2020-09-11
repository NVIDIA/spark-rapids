# RAPIDS Plugin For Apache Spark Integration Tests

This is a set of integration tests for the RAPIDS Plugin for Apache Spark. These tests
are intended to be able to be run against any Spark-compatible cluster/release to help
verify that the plugin is doing the right thing in as many cases as possible.

There are two sets of tests here. The pyspark tests are described here. The scala tests
are described [here](../docs/testing.md)

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

### pandas
`pip install pandas`

`pandas` is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool.

### pyarrow
`pip install pyarrow`

`pyarrow` provides a Python API for functionality provided by the Arrow C++ libraries, along with tools for Arrow integration and interoperability with pandas, NumPy, and other software in the Python ecosystem.

## Running

Running the tests follows the pytest conventions, the main difference is using
`spark-submit` to launch the tests instead of pytest.

```
$SPARK_HOME/bin/spark-submit ./runtests.py
```

See `pytest -h` or `$SPARK_HOME/bin/spark-submit ./runtests.py -h` for more options.

Most clusters probably will not have the RAPIDS plugin installed in the cluster yet.
If just want to verify the SQL replacement is working you will need to add the `rapids-4-spark` and `cudf` jars to your `spark-submit` command.

```
$SPARK_HOME/bin/spark-submit --jars "rapids-4-spark_2.12-0.2.0-SNAPSHOT.jar,cudf-0.15.jar" ./runtests.py
```

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

```
$SPARK_HOME/bin/spark-submit --jars "rapids-4-spark_2.12-0.2.0-SNAPSHOT.jar,cudf-0.15.jar,rapids-4-spark-tests_2.12-0.2.0-SNAPSHOT.jar" ./runtests.py --tpcxbb_format="csv" --tpcxbb_path="/path/to/tpcxbb/csv"
```

## Writing tests

There are a number of libraries provided to help someone write new tests.

### `data_gen.py`

`data_gen` allow you to generate data for various spark types. There tends to be a `${Type}Gen` class for every `${Type}Type` class supported by the plugin.  Each of these
has decent default values, so a `DoubleGen` should produce a random set of `DoubleType` including most corner cases like `-0.0`, `NaN`, `Inf` and `-Inf`.

Many of the classes also allow for some customization of the data produced.

All of the classes allow you to add in your own corner cases using `with_special_case`.  So if you want an `IntegerGen` that produces a lot of 5s you could run.

```
IntegerGen().with_special_case(5, weight=200)
```

The value passed in can be a constant value, or it can be a function that takes a `random.Random` instance to generate the data. The `weight` lets you
set a relative priority compared with other data.  By default the randomly distributed data has a weight of 100, and special cases have a weight of 1.

Not everything is documented here and you should look around at the library to see what it supports. Feel free to modify it to add in new options
but be careful because we want to maintain good data coverage.

To generate data from a list of `*Gen` instances or from a single `StructGen` you can use the `gen_df` function.

### `asserts.py` and `marks.py`

`asserts.py` provides 2 APIs that let you run a command and verify that it produced the "same" result on both the CPU and the GPU.

`assert_gpu_and_cpu_are_equal_collect` and `assert_gpu_and_cpu_are_equal_iterator` the difference is in how the results are brought back for
verification. Most tests should use the collect version. If you have a test that will produce a very large amount of data you can use the
iterator version, but it will be much slower.

Each of these take a function as input.  The function will be passed an instance of `spark` that is configured for the given environment
and it should return a data frame.  It also takes a dictionary of config entries that can be set when the test runs.  Most config entries
you care about should be through markers.

pytest uses markers to tag different tests with metadata. This framework uses them to be able to set various configs when running the tests.
Markers were chosen because it provides a lot of flexibility even with parameterized tests.

The marks you care about are all in marks.py

   * `ignore_order` tells the asserts to sort the resulting data because the tests may not produce the results in the same order
   * `incompat` tells the tests to enable incompat operators. It does not enable approximate comparisons for floating point though.
   * `approximate_float` tells the tests to compare floating point values (including double) and allow for an error. This follows `pytest.approx` and will also take `rel` and `abs` args.
   * `allow_non_gpu` tells the tests that not everything in the query will run on the GPU. You can tell it to allow all CPU fallback by `@allow_non_gpu(any=True)` you can also pass in class names that are enabled for CPU operation.

###  `spark_session.py`

For the most part you can ignore this file. It provides the underlying Spark session to operations that need it, but most tests should interact with
it through `asserts.py`.

