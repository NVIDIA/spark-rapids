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
This will build the plugin for a single version of Spark.  By default this is Apache Spark
3.1.1. To build against other versions of Spark you use the `-Dbuildver=XXX` command line option
to Maven. For instance to build Spark 3.1.1 you would use:

```shell script
mvn -Dbuildver=311 verify
```
You can find all available build versions in the top level pom.xml file. If you are building
for Databricks then you should use the `jenkins/databricks/build.sh` script and modify it for
the version you want.

To get an uber jar with more than 1 version you have to `mvn package` each version
and then use one of the defined profiles in the dist module, or a comma-separated list of
build versions. See the next section for more details.

### Building a Distribution for Multiple Versions of Spark

By default the distribution jar only includes code for a single version of Spark. If you want
to create a jar with multiple versions we have the following options.

1. Build for all Apache Spark versions and CDH with no SNAPSHOT versions of Spark, only released. Use `-PnoSnapshots`.
2. Build for all Apache Spark versions and CDH including SNAPSHOT versions of Spark we have supported for. Use `-Psnapshots`.
3. Build for all Apache Spark versions, CDH and Databricks with no SNAPSHOT versions of Spark, only released. Use `-PnoSnaphsotsWithDatabricks`.
4. Build for all Apache Spark versions, CDH and Databricks including SNAPSHOT versions of Spark we have supported for. Use `-PsnapshotsWithDatabricks`
5. Build for an arbitrary combination of comma-separated build versions using `-Dincluded_buildvers=<CSV list of build versions>`.
   E.g., `-Dincluded_buildvers=312,330`

You must first build each of the versions of Spark and then build one final time using the profile for the option you want.

You can also install some manually and build a combined jar. For instance to build non-snapshot versions:

```shell script
mvn clean
mvn -Dbuildver=311 install -Drat.skip=true -DskipTests
mvn -Dbuildver=312 install -Drat.skip=true -DskipTests
mvn -Dbuildver=313 install -Drat.skip=true -DskipTests
mvn -Dbuildver=320 install -Drat.skip=true -DskipTests
mvn -Dbuildver=321 install -Drat.skip=true -DskipTests
mvn -Dbuildver=321cdh install -Drat.skip=true -DskipTests
mvn -pl dist -PnoSnapshots package -DskipTests
```
#### Building with buildall script

There is a build script `build/buildall` that automates the local build process. Use
`./buid/buildall --help` for up-to-date use information.

By default, it builds everything that is needed to create a distribution jar for all released (noSnapshots) Spark versions except for Databricks. Other profiles that you can pass using `--profile=<distribution profile>` include
- `snapshots` that includes all released (noSnapshots) and snapshots Spark versions except for Databricks
- `minimumFeatureVersionMix` that currently includes 321cdh, 312, 320, 330 is recommended for catching incompatibilities already in the local development cycle

For initial quick iterations we can use `--profile=<buildver>` to build a single-shim version. e.g., `--profile=311` for Spark 3.1.1.

The option `--module=<module>` allows to limit the number of build steps. When iterating, we often don't have the need for the entire build. We may be interested in building everything necessary just to run integration tests (`--module=integration_tests`), or we may want to just rebuild the distribution jar (`--module=dist`)

By default, `buildall` builds up to 4 shims in parallel using `xargs -P <n>`. This can be adjusted by
specifying the environment variable `BUILD_PARALLEL=<n>`.

### Building against different CUDA Toolkit versions

You can build against different versions of the CUDA Toolkit by using qone of the following profiles:
* `-Pcuda11` (CUDA 11.0/11.1/11.2, default)

### Building and Testing with JDK9+
We support JDK8 as our main JDK version. However, it's possible to build and run with more modern
JDK versions as well. To this end set `JAVA_HOME` in the environment to your JDK root directory.

With JDK9+, you need to disable the default classloader manipulation option and set
spark.rapids.force.caller.classloader=false in your Spark application configuration. There are, however,
known issues with it, e.g. see #5513.

At the time of this writing, the most robust way to run the RAPIDS Accelerator is from a jar dedicated to
a single Spark version. To this end please use a single shim and specify `-DallowConventionalDistJar=true`

Also make sure to use scala-maven-plugin version `scala.plugin.version` 4.6.0 or later to correctly process
[maven.compiler.release](https://github.com/davidB/scala-maven-plugin/blob/4.6.1/src/main/java/scala_maven/ScalaMojoSupport.java#L161)
flag if cross-compilation is required.

```bash
mvn clean verify -Dbuildver=321 \
  -Dmaven.compiler.release=11 \
  -Dmaven.compiler.source=11 \
  -Dmaven.compiler.target=11 \
  -Dscala.plugin.version=4.6.1 \
  -DallowConventionalDistJar=true
```

## Code contributions

### Source code layout

Conventional code locations in Maven modules are found under `src/main/<language>`. In addition to
that and in order to support multiple versions of Apache Spark with the minimum amount of source
code we maintain Spark-version-specific locations within non-shim modules if necessary. This allows
us to switch between incompatible parent classes inside without copying the shared code to
dedicated shim modules.

Thus, the conventional source code root directories `src/main/<language>` contain the files that
are source-compatible with all supported Spark releases, both upstream and vendor-specific.

The following acronyms may appear in directory names:

|Acronym|Definition  |Example|Example Explanation                           |
|-------|------------|-------|----------------------------------------------|
|db     |Databricks  |312db  |Databricks Spark based on Spark 3.1.2         |
|cdh    |Cloudera CDH|321cdh |Cloudera CDH Spark based on Apache Spark 3.2.1|

The version-specific directory names have one of the following forms / use cases:
- `src/main/312/scala` contains Scala source code for a single Spark version, 3.1.2 in this case
- `src/main/312+-apache/scala`contains Scala source code for *upstream* **Apache** Spark builds,
   only beginning with version Spark 3.1.2, and + signifies there is no upper version boundary
   among the supported versions
- `src/main/311until320-all` contains code that applies to all shims between 3.1.1 *inclusive*,
3.2.0 *exclusive*
- `src/main/pre320-treenode` contains shims for the Catalyst `TreeNode` class before the
  [children trait specialization in Apache Spark 3.2.0](https://issues.apache.org/jira/browse/SPARK-34906).
- `src/main/post320-treenode` contains shims for the Catalyst `TreeNode` class after the
  [children trait specialization in Apache Spark 3.2.0](https://issues.apache.org/jira/browse/SPARK-34906).


### Setting up an Integrated Development Environment

Our project currently uses `build-helper-maven-plugin` for shimming against conflicting definitions of superclasses
in upstream versions that cannot be resolved without significant code duplication otherwise. To this end different
source directories with differently implemented same-named classes are
[added](https://www.mojohaus.org/build-helper-maven-plugin/add-source-mojo.html)
for compilation depending on the targeted Spark version.

This may require some modifications to IDEs' standard Maven import functionality.

#### IntelliJ IDEA

_Last tested with 2021.2.1 Community Edition_

To start working with the project in IDEA is as easy as
[opening](https://blog.jetbrains.com/idea/2008/03/opening-maven-projects-is-easy-as-pie/) the top level (parent)
[pom.xml](pom.xml).

In order to make sure that IDEA handles profile-specific source code roots within a single Maven module correctly,
[unselect](https://www.jetbrains.com/help/idea/2021.2/maven-importing.html) "Keep source and test folders on reimport".

If you develop a feature that has to interact with the Shim layer or simply need to test the Plugin with a different
Spark version, open [Maven tool window](https://www.jetbrains.com/help/idea/2021.2/maven-projects-tool-window.html) and
select one of the `release3xx` profiles (e.g, `release320`) for Apache Spark 3.2.0, and click "Reload"
if not triggered automatically.

There is a known issue where, even after selecting a different Maven profile in the Maven submenu, the source folders from
a previously selected profile may remain active. To get around this you have to manually reload the Maven project from
the Maven side menu.

If you see Scala symbols unresolved (highlighted red) in IDEA please try the following steps to resolve it:
- Make sure there are no relevant poms in "File->Settings->Build Tools->Maven->Ignored Files"
- Restart IDEA and click "Reload All Maven Projects" again

#### Bloop Build Server

[Bloop](https://scalacenter.github.io/bloop/) is a build server and a set of tools around Build
Server Protocol (BSP) for Scala providing an integration path with IDEs that support it. In fact,
you can generate a Bloop project from Maven just for the Maven modules and profiles you are
interested in. For example, to generate the Bloop projects for the Spark 3.2.0 dependency
just for the production code run:

```shell script
mvn install ch.epfl.scala:maven-bloop_2.13:1.4.9:bloopInstall -pl aggregator -am \
  -DdownloadSources=true \
  -Dbuildver=320 \
  -DskipTests \
  -Dskip \
  -Dmaven.javadoc.skip \
  -Dmaven.scalastyle.skip=true \
  -Dmaven.updateconfig.skip=true
```

With `--generate-bloop` we integrated Bloop project generation into `buildall`. It makes it easier
to generate projects for multiple Spark dependencies using the same profiles as our regular build.
It makes sure that the project files belonging to different Spark dependencies are
not clobbered by repeated `bloopInstall` Maven plugin invocations, and it uses
[jq](https://stedolan.github.io/jq/) to post-process JSON-formatted project files such that they
compile project classes into non-overlapping set of output directories.

You can now open the spark-rapids as a
[BSP project in IDEA](https://www.jetbrains.com/help/idea/bsp-support.html)

# Bloop, Scala Metals, and Visual Studio Code

_Last tested with 1.63.0-insider (Universal) Commit: bedf867b5b02c1c800fbaf4d6ce09cefba_

Another, and arguably more popular, use of Bloop arises in connection with
[Scala Metals](https://scalameta.org/metals/) and [VS @Code](https://code.visualstudio.com/).
Scala Metals implements the
[Language Server Protocol (LSP)](https://microsoft.github.io/language-server-protocol/) for Scala,
and enables features such as context-aware autocomplete, and code browsing between Scala symbol
definitions, references and vice versa. LSP is supported by many editors including Vim and Emacs.

Here we document the integration with VS code. It makes development on a remote node almost
as easy as local development, which comes very handy when working in Cloud environments.

Run `./build/buildall --generate-bloop --profile=<profile>` to generate Bloop projects
for required Spark dependencies, e.g. `--profile=320` for Spark 3.2.0. When developing
remotely this is done on the remote node.

Install [Scala Metals extension](https://scalameta.org/metals/docs/editors/vscode) in VS Code,
either locally or into a Remote-SSH extension destination depending on your target environment.
When your project folder is open in VS Code, it may prompt you to import Maven project.
IMPORTANT: always decline with "Don't ask again", otherwise it will overwrite the Bloop projects
generated with the default `311` profile. If you need to use a different profile, always rerun the
command above manually. When regenerating projects it's recommended to proceed to Metals
"Build commands" View, and click:
1. "Restart build server"
1. "Clean compile workspace"
to avoid stale class files.

Now you should be able to see Scala class members in the Explorer's Outline view and in the
Breadcrumbs view at the top of the Editor with a Scala file open.

Check Metals logs, "Run Doctor", etc if something is not working as expected. You can also verify
that the Bloop build server and the Metals language server are running by executing `jps` in the
Terminal window:
```shell script
jps -l
72960 sun.tools.jps.Jps
72356 bloop.Server
72349 scala.meta.metals.Main
```

#### Other IDEs
We welcome pull requests with tips how to setup your favorite IDE!

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

When automatic copyright updater is enabled and you modify a file with a prior
year in the copyright header it will be updated on `git commit` to the current year automatically.
However, this will abort the [commit process](https://github.com/pre-commit/pre-commit/issues/532)
with the following error message:
```
Update copyright year....................................................Failed
- hook id: auto-copyrighter
- duration: 0.01s
- files were modified by this hook
```
You can confirm that the update actually has happened by either inspecting its effect with
`git diff` first or simply reexecuting `git commit` right away. The second time no file
modification should be triggered by the copyright year update hook and the commit should succeed.

### Pull request status checks
A pull request should pass all status checks before merged.
#### signoff check
Please follow the steps in the [Sign your work](#sign-your-work) section,
and make sure at least one commit in your pull request get signed-off.
#### blossom-ci
The check runs on NVIDIA self-hosted runner, a [project committer](.github/workflows/blossom-ci.yml#L36) can
manually trigger it by commenting `build`. It includes following steps,
1. Mergeable check
2. Blackduck vulnerability scan
3. Fetch merged code (merge the pull request HEAD into BASE branch, e.g. fea-001 into branch-x)
4. Run `mvn verify` and unit tests for multiple Spark versions in parallel.
Ref: [spark-premerge-build.sh](jenkins/spark-premerge-build.sh)

If it fails, you can click the `Details` link of this check, and go to `Upload log -> Jenkins log for pull request xxx (click here)` to
find the uploaded log.

Options:
1. Skip tests run by adding `[skip ci]` to title, this should only be used for doc-only change
2. Run build and tests in databricks runtimes by adding `[databricks]` to title, this would add around 30-40 minutes

## Attribution
Portions adopted from https://github.com/rapidsai/cudf/blob/main/CONTRIBUTING.md, https://github.com/NVIDIA/nvidia-docker/blob/master/CONTRIBUTING.md, and https://github.com/NVIDIA/DALI/blob/main/CONTRIBUTING.md
