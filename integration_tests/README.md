# RAPIDS Plugin For Apache Spark Integration Tests

This is a set of integration tests for the RAPIDS Plugin for Apache Spark. These tests
are intended to be able to be run against any Spark-compatible cluster/release to help
verify that the plugin is doing the right thing in as many cases as possible.

There are two sets of tests here. The pyspark tests are described here. The scala tests
are described [here](../tests/README.md)

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

`pandas` is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool and is
only needed when testing integration with pandas.

### pyarrow
`pip install pyarrow`

`pyarrow` provides a Python API for functionality provided by the Arrow C++ libraries, along with tools for Arrow
integration and interoperability with pandas, NumPy, and other software in the Python ecosystem. This is used
to test improved transfer performance to pandas based user defined functions.

## pytest-xdist and findspark

`pytest-xdist` and `findspark` can be used to speed up running the tests by running them in parallel.

## Running

Tests will run as a part of the maven build if you have the environment variable `SPARK_HOME` set.

The suggested way to run these tests is to use the shell-script file located in the
 integration_tests folder called [run_pyspark_from_build.sh](run_pyspark_from_build.sh). This script takes 
care of some of the flags that are required to run the tests which will have to be set for the 
plugin to work. It will be very useful to read the contents of the 
[run_pyspark_from_build.sh](run_pyspark_from_build.sh) to get a better insight 
into what is needed as we constantly keep working on to improve and expand the plugin-support.

The python tests run with pytest and the script honors pytest parameters. Some handy flags are:
- `-k` <pytest-file-name>. This will run all the tests in that test file.
- `-k` <test-name>. This will also run an individual test.
- `-s` Doesn't capture the output and instead prints to the screen.
- `-v` Increase the verbosity of the tests
- `-r fExXs` Show extra test summary info as specified by chars: (f)ailed, (E)rror, (x)failed, (X)passed, (s)kipped
- For other options and more details please visit [pytest-usage](https://docs.pytest.org/en/stable/usage.html) or type `pytest --help`

By default the tests try to use the python packages `pytest-xdist` and `findspark` to oversubscribe
your GPU and run the tests in Spark local mode. This can speed up these tests significantly as all
of the tests that run by default process relatively small amounts of data. Be careful because if
you have `SPARK_CONF_DIR` also set the tests will try to use whatever cluster you have configured.
If you do want to run the tests in parallel on an existing cluster it is recommended that you set
`-Dpytest.TEST_PARALLEL` to one less than the number of worker applications that will be
running on the cluster.  This is because `pytest-xdist` will launch one control application that
is not included in that number. All it does is farm out work to the other applications, but because
it needs to know about the Spark cluster to determine which tests to run and how it still shows up
as a Spark application.

To run the tests separate from the build go to the `integration_tests` directory. You can submit
`runtests.py` through `spark-submit`, but if you want to run the tests in parallel with
`pytest-xdist` you will need to submit it as a regular python application and have `findspark`
installed.  Be sure to include the necessary jars for the RAPIDS plugin either with
`spark-submit` or with the cluster when it is 
[setup](../docs/get-started/getting-started-on-prem.md).
The command line arguments to `runtests.py` are the same as for 
[pytest](https://docs.pytest.org/en/latest/usage.html). The only reason we have a separate script
is that `spark-submit` uses python if the file name ends with `.py`.

If you want to configure the Spark cluster you may also set environment variables for the tests.
The name of the env var should be in the form `"PYSP_TEST_" + conf_key.replace('.', '_')`. Linux
does not allow '.' in the name of an environment variable so we replace it with an underscore. As
Spark configs avoid this character we have no other special processing.

We also have a large number of integration tests that currently run as a part of the unit tests
using scala test. Those are in the `src/test/scala` sub-directory and depend on the testing
framework from the `rapids-4-spark-tests_2.12` test jar.

You can run these tests against a cluster similar to how you can run `pytests` against an
existing cluster. To do this you need to launch a cluster with the plugin jars on the
classpath. The tests will enable and disable the plugin as they run.

Next you need to copy over some test files to whatever distributed file system you are using.
The test files are everything under `./integration_tests/src/test/resources/`  Be sure to note
where you placed them because you will need to tell the tests where they are.

When running these tests you will need to include the test jar, the integration test jar,
the udf-examples jar, scala-test and scalactic. You can find scala-test and scalactic under
`~/.m2/repository`.

It is recommended that you use `spark-shell` and the scalatest shell to run each test
individually, so you don't risk running unit tests along with the integration tests.
http://www.scalatest.org/user_guide/using_the_scalatest_shell

```shell 
spark-shell --jars rapids-4-spark-tests_2.12-0.5.0-tests.jar,rapids-4-spark-udf-examples_2.12-0.5.0,rapids-4-spark-integration-tests_2.12-0.5.0-tests.jar,scalatest_2.12-3.0.5.jar,scalactic_2.12-3.0.5.jar
```

First you import the `scalatest_shell` and tell the tests where they can find the test files you
just copied over.

```scala
import org.scalatest._
com.nvidia.spark.rapids.TestResourceFinder.setPrefix(PATH_TO_TEST_FILES)
```

Next you can start to run the tests.

```scala
durations.run(new com.nvidia.spark.rapids.JoinsSuite)
...
```

Most clusters probably will not have the RAPIDS plugin installed in the cluster yet.
If you just want to verify the SQL replacement is working you will need to add the
`rapids-4-spark` and `cudf` jars to your `spark-submit` command. Note the following
example assumes CUDA 10.1 is being used.

```
$SPARK_HOME/bin/spark-submit --jars "rapids-4-spark_2.12-0.5.0.jar,rapids-4-spark-udf-examples_2.12-0.5.0.jar,cudf-0.19.1-cuda10-1.jar" ./runtests.py
```

You don't have to enable the plugin for this to work, the test framework will do that for you.


You do need to have access to a compatible GPU with the needed CUDA drivers. The exact details of how to set this up are beyond the scope of this document, but the Spark feature for scheduling GPUs does make this very simple if you have it configured.

### Runtime Environment

`--runtime_env` is used to specify the environment you are running the tests in. Valid values are `databricks`,`emr`,`dataproc` and `apache`. This is generally used
when certain environments have different behavior, and the tests don't have a good way to auto-detect the environment yet.

### timezone

The RAPIDS plugin currently only supports the UTC time zone. Spark uses the default system time zone unless explicitly set otherwise.
To make sure that the tests work properly you need to configure your cluster or application to run with UTC.
The python framework cannot always do this for you because it risks overwriting other java options in the config.
Please be sure that the following configs are set when running the tests.

  * `spark.driver.extraJavaOptions` should include `-Duser.timezone=UTC`
  * `spark.executor.extraJavaOptions` should include `-Duser.timezone=UTC`
  * `spark.sql.session.timeZone`=`UTC`

### Running in parallel

You may use `pytest-xdist` to run the tests in parallel. This is done by running the tests through `python`, not `spark-submit`,
and setting the parallelism with the `-n` command line parameter. Be aware that `pytest-xdist` will launch one control application
and the given number of worker applications, so your cluster needs to be large enough to handle one more application than the parallelism
you set. Most tests are small and don't need even a full GPU to run. So setting your applications to use a single executor and a single
GPU per executor is typically enough. When running from maven we assume that we are running in local mode and will try to
oversubscribe a single GPU.  Typically we find that the tests don't need more than 2GB of GPU memory so we can speed up the tests significantly
by doing this. It is not easy nor recommended to try and configure an actual cluster so you can oversubscribe GPUs.  Please don't try it.

Under YARN and Kubernetes you can set `spark.executor.instances` to the number of executors you want running in your application
(1 typically). Spark will auto launch a driver for each application too, but if you configured it correctly that would not take
any GPU resources on the cluster. For standalone, Mesos, and Kubernetes you can set `spark.cores.max` to one more than the number
of executors you want to use per application. The extra core is for the driver. Dynamic allocation can mess with these settings
under YARN and even though it is off by default you probably want to be sure it is disabled (spark.dynamicAllocation.enabled=false).

### Running with Alternate Paths

In case your test jars and resources are downloaded to the `local-path` from dependency Repo, and you want to run tests with them
using the shell-script [run_pyspark_from_build.sh](run_pyspark_from_build.sh), then the `LOCAL_JAR_PATH=local-path` must be set to point
to the `local-path`, e.g. `LOCAL_JAR_PATH=local-path bash [run_pyspark_from_build.sh](run_pyspark_from_build.sh)`.By setting `LOCAL_JAR_PATH=local-path`
the shell-script [run_pyspark_from_build.sh](run_pyspark_from_build.sh) can find the test jars and resources in the alternate path.

When running the shell-script [run_pyspark_from_build.sh](run_pyspark_from_build.sh) under YARN or Kubernetes, the `$SCRIPTPATH` in the python options
`--rootdir $SCRIPTPATH ...` and `--std_input_path $SCRIPTPATH ...` will not work, as the `$SCRIPTPATH` is a local path, you need to overwrite it to the clould paths.
Basically, you need first to upload the test resources onto the cloud path `resource-path`, then transfer the test resources onto the working directory
`root-dir` of each executor(e.g. via `spark-submit --files root-dir ...`). After that you must set both `LOCAL_ROOTDIR=root-dir` and `INPUT_PATH=resource-path`
to run the shell-script, e.g. `LOCAL_ROOTDIR=root-dir INPUT_PATH=resource-path bash [run_pyspark_from_build.sh](run_pyspark_from_build.sh)`.

### Enabling cudf_udf Tests

The cudf_udf tests in this framework are testing Pandas UDF(user-defined function) with cuDF. They are disabled by default not only because of the complicated environment setup, but also because GPU resources scheduling for Pandas UDF is an experimental feature now, the performance may not always be better.
The tests can be enabled by just appending the option `--cudf_udf` to the command.

   * `--cudf_udf` (enable the cudf_udf tests when provided, and remove this option if you want to disable the tests)

cudf_udf tests needs a couple of different settings, they may need to run separately.

To enable cudf_udf tests, need following pre requirements:
   * Install cuDF Python library on all the nodes running executors. The instruction could be found at [here](https://rapids.ai/start.html). Please follow the steps to choose the version based on your environment and install the cuDF library via Conda or use other ways like building from source.
   * Disable the GPU exclusive mode on all the nodes running executors. The sample command is `sudo nvidia-smi -c DEFAULT`
   
To run cudf_udf tests, need following configuration changes:   
   * Add configurations `--py-files` and `spark.executorEnv.PYTHONPATH` to specify the plugin jar for python modules 'rapids/daemon' 'rapids/worker'.
   * Decrease `spark.rapids.memory.gpu.allocFraction` to reserve enough GPU memory for Python processes in case of out-of-memory.
   * Add `spark.rapids.python.concurrentPythonWorkers` and `spark.rapids.python.memory.gpu.allocFraction` to reserve enough GPU memory for Python processes in case of out-of-memory.

As an example, here is the `spark-submit` command with the cudf_udf parameter on CUDA 10.1:

```
$SPARK_HOME/bin/spark-submit --jars "rapids-4-spark_2.12-0.5.0.jar,rapids-4-spark-udf-examples_2.12-0.5.0.jar,cudf-0.19.1-cuda10-1.jar,rapids-4-spark-tests_2.12-0.5.0.jar" --conf spark.rapids.memory.gpu.allocFraction=0.3 --conf spark.rapids.python.memory.gpu.allocFraction=0.3 --conf spark.rapids.python.concurrentPythonWorkers=2 --py-files "rapids-4-spark_2.12-0.5.0.jar" --conf spark.executorEnv.PYTHONPATH="rapids-4-spark_2.12-0.5.0.jar" ./runtests.py --cudf_udf
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
