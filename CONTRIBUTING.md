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

## Branching Convention

There are two types of branches in this repository:

* `branch-[version]`: are development branches which can change often. Note that we merge into
  the branch with the greatest version number, as that is our default branch.

* `main`: is the branch with the latest released code, and the version tag (i.e. `v0.1.0`)
  is held here. `main` will change with new releases, but otherwise it should not change with
  every pull request merged, making it a more stable branch.

## Building From Source

We use [Maven](https://maven.apache.org) for most aspects of the build. Some important parts
of the build execute in the `verify` phase of the Maven build lifecycle.  We recommend when
building at least running to the `verify` phase, e.g.:

```shell script
mvn verify
```

After a successful build the RAPIDS Accelerator jar will be in the `dist/target/` directory.

### Building against different CUDA Toolkit versions

You can build against different versions of the CUDA Toolkit by using one of the following profiles:
* `-Pcuda11` (CUDA 11.0/11.1/11.2, default)

## Code contributions

### Your first issue

1. Read the [Developer Overview](docs/dev/README.md) to understand how the RAPIDS Accelerator
    plugin works.
2. Find an issue to work on. The best way is to look for the
    [good first issue](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
    or [help wanted](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
    labels.
3. Comment on the issue stating that you are going to work on it.
4. Code! Make sure to update unit tests and integration tests if needed! [refer to test section](#testing-your-code)
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

### Testing Your Code
Please visit the [testing doc](tests/README.md) for details about how to run tests


### Pre-commit hooks
We provide a basic config `.pre-commit-config.yaml` for [pre-commit](https://pre-commit.com/) to
automate some aspects of the development process. As a convenience you can enable automatic 
copyright year updates by following the installation instructions on the
[pre-commit homepage](https://pre-commit.com/).

To this end, first install `pre-commit` itself using the method most suitable for your development
environment. Then you will need to run `pre-commit install` to enable it in your local git
repository. Using `--allow-missing-config` will make it easy to work with older branches
that do not have `.pre-commit-config.yaml`.

```bash
pre-commit install --allow-missing-config
```

and setting the environment variable:

```bash
export SPARK_RAPIDS_AUTO_COPYRIGHTER=ON
```
The default value of `SPARK_RAPIDS_AUTO_COPYRIGHTER` is `OFF`.

## Attribution
Portions adopted from https://github.com/rapidsai/cudf/blob/main/CONTRIBUTING.md, https://github.com/NVIDIA/nvidia-docker/blob/main/CONTRIBUTING.md, and https://github.com/NVIDIA/DALI/blob/main/CONTRIBUTING.md
