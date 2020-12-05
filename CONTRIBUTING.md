# Contributing to RAPIDS Accelerator for Apache Spark

Contributions to RAPIDS Accelerator for Apache Spark fall into the following three categories.

1. To report a bug, request a new feature, or report a problem with
    documentation, please file an [issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose)
    describing in detail the problem or new feature. The project team evaluates
    and triages issues, and schedules them for a release. If you believe the
    issue needs priority attention, please comment on the issue to notify the
    team.
2. To propose and implement a new Feature, please file a new feature request
    [issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose). Describe the
    intended feature and discuss the design and implementation with the team and
    community. Once the team agrees that the plan looks good, go ahead and
    implement it using the [code contributions](#code-contributions) guide below.
3. To implement a feature or bug-fix for an existing outstanding issue, please
    follow the [code contributions](#code-contributions) guide below. If you
    need more context on a particular issue, please ask in a comment.

## Code contributions

### Your first issue

1. Read the [Developer Overview](docs/dev/README.md) to understand how the RAPIDS Accelerator
    plugin works.
2. Find an issue to work on. The best way is to look for the
    [good first issue](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
    or [help wanted](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
    labels.
3. Comment on the issue stating that you are going to work on it.
4. Code! Make sure to update unit tests and integration tests if needed! [refer to test section](#test)
5. When done, [create your pull request](https://github.com/NVIDIA/spark-rapids/compare).
6. Verify that CI passes all [status checks](https://help.github.com/articles/about-status-checks/).
    Fix if needed.
7. Wait for other developers to review your code and update code as needed.
8. Once reviewed and approved, a project committer will merge your pull request.

Remember, if you are unsure about anything, don't hesitate to comment on issues
and ask for clarifications!

### Code Formatting
RAPIDS Accelerator for Apache Spark follows the same coding style guidelines as the Apache Spark
project.  For IntelliJ IDEA users, an
[example code style settings file](docs/dev/idea-code-style-settings.xml) is available in the
`docs/dev/` directory.

#### Scala

This project follows the official
[Scala style guide](https://docs.scala-lang.org/style/) and the
[Databricks Scala guide](https://github.com/databricks/scala-style-guide), preferring the latter.

#### Java

This project follows the
[Oracle Java code conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html)
and the Scala conventions detailed above, preferring the latter.

### Sign your work

We require that all contributors sign-off on their commits. This certifies that the contribution is your original work, or you have rights to submit it under the same license, or a compatible license.

Any contribution which contains commits that are not signed off will not be accepted.

To sign off on a commit use the `--signoff` (or `-s`) option when committing your changes:

```shell
git commit -s -m "Add cool feature."
```

This will append the following to your commit message:

```
Signed-off-by: Your Name <your@email.com>
```

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. Use your real name, no pseudonyms or anonymous contributions.  If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.


The signoff means you certify the below (from [developercertificate.org](https://developercertificate.org)):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### Testing your code
There are two types of tests in this project. Unit tests that are written in Scala, and integration tests that are written in Python. 
We encourage writing integration tests if you have to choose between the two as that helps us ensure the plugin is working across different platforms. 

#### Unit tests
Unit-tests are located [here](tests). In order to run the unit-tests follow these steps
1. Issue the Maven command to run the tests with `mvn test`. This will run all the tests in the tests module. 
2. To run individual tests append `-DwildcardSuites=<comma separated list of wildcard suite names to execute>` to the above command 

For more information about using scalatest with maven please refere [here](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin)
    
##### Running unit-tests against specific Apache Spark versions. 
There are Maven profiles that should be used to run Apache Spark version-specific tests e.g. To run tests against Apache Spark 3.1.0, 
`mvn -P spark310tests test -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"`. 
Please refer to the [pom.xml](tests/pom.xml) to see the list of profiles supported

Apache Spark specific configurations can be passed in by setting environment-variable SPARK_CONF 

Examples: 
- To pass Apache Spark configs `--conf spark.dynamicAllocation.enabled=false --conf spark.task.cpus=1` do something like.
`SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn ...`
- To run test ParquetWriterSuite in package com.nvidia.spark.rapids, issue `mvn test -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"`


#### Integration tests
Integration tests are located [here](integration_tests). The suggested way to run these tests is to use the shell-script file located in the module folder called
[run_pyspark_from_build.sh](run_pyspark_from_build.sh). This script takes care of some of the flags that are required to run the tests which
will have to be set for the plugin to work. It will be very useful to read the contents of the [run_pyspark_from_build.sh](run_pyspark_from_build.sh) to get
better insight into what is needed as we constantly keep working on to improve and expand the plugin-support.

The tests are written python and run with pytest and the script honors pytest parameters. Some handy flags are:
- `-k` <pytest-file-name>. This will run all the tests in that test file.
- `-k` <test-name>. This will also run an individual test.
- `-s` Doesn't capture the output and instead prints to the screen.
- `-v` increase verbosity of the tests
- `-rfExXs` show extra test summary info as specified by chars: (f)ailed, (E)rror, (x)failed, (X)passed, (s)kipped
- For other options and more details please visit [pytest-usage](https://docs.pytest.org/en/stable/usage.html) or type `pytest --help`

Example: 
- This command runs all the tests located in `cache_test.py` against Apache Spark 3.1.0 using the ParquetCachedBatchSerializer and other configs discussed above and with the debugger listening on port 5005
`SPARK_SUBMIT_FLAGS="--driver-memory 4g --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark310.ParquetCachedBatchSerializer" 
SPARK_HOME=~/spark-3.1.0-SNAPSHOT-bin-hadoop3.2/ 
COVERAGE_SUBMIT_FLAGS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005' ./run_pyspark_from_build.sh -k cache_test`

## Attribution
Portions adopted from https://github.com/rapidsai/cudf/blob/main/CONTRIBUTING.md, https://github.com/NVIDIA/nvidia-docker/blob/main/CONTRIBUTING.md, and https://github.com/NVIDIA/DALI/blob/main/CONTRIBUTING.md  
