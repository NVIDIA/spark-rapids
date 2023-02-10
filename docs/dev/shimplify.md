---
layout: page
title: Shim Source Code Layout Simplification with Shimplify
nav_order: 8
parent: Developer Overview
---

# Shim Source Code Layout Simplification with Shimplify

This document describes the next iteration of shim source code maintenance. It addresses the
drawback introduced with the [shim layer rework][1] resulting in the guaranteed ABI-compatible
bytecode management for each of the 14 currently supported Spark builds but at the expense of
maintaining 50+ directories. Many shims are spread over an overlapping set of directories making it
hard to determine where to make additions while keeping code duplication in check.

[shimplify.py][2] is the new goal in the Maven build binding to the `generate-sources` phase.

* It defines a new simpler shim directory structure where there is only a single directory per shim,
and a special comment is injected to define metadata defining all the shim builds it participates in.
* It can convert all or a subset of existing shims to the new build. The build can support partially
converted shims if a longer transition is desired.

## Simplified Shim Source Directory Structure

In our build each supported Apache Spark build and its corresponding Shim is identified by its
[`buildver`][3] property. Every Maven submodule requiring shimming (`sql-plugin`, `tests` as of the
time of this writing) have a new set of special sibling directories
`src/(main|test)/spark${buildver}`.

Previous `src/(main|test)/${buildver}` and
version-range-with-exceptions directories such as `src/main/311until340-non330db` are deprecated and
will be removed soon as a result of the conversion to the new structure.

`shimplify` changes the way the source code is shared among Shims by using an explicit
lexicographically sorted list of `buildver` property values
in a source-code level comment instead of the shared directories.

```scala
/*** spark-rapids-shim-json-lines
{"spark": "312"}
{"spark": "323"}
spark-rapids-shim-json-lines ***/
```

The content inside the tags `spark-rapids-shim-json-lines` is in the [JSON Lines][4] format where
each line is an extensible object with the Shim metadata currently consisting just of the Spark
build dependency version. The top object in the comment, the minimum version in the comment
intuitively represents the first version of Spark requiring shimming in the plugin, albeit it might
not be the original one as support for older Spark releases is eventually dropped. This `buildver`
is called the *owner shim*.

On the default read-only invocation path of the Maven build shimplify does not make any changes to
shim source code files and their locations.

* It analyzes the pre-shimplify directory structure and identifies the shims that through the code
evolution ended up using more dedicated directories than necessary contributing avoidable
complexity on top of an inherently complex directory structure already. As an example this is one of
such warnings:

```text
shimplify - WARNING - Consider consolidating 312db, it spans multiple dedicated directories ['/home/user/gits/NVIDIA/spark-rapids/sql-plugin/src/main/312db/scala', '/home/user/gits/NVIDIA/spark-rapids/sql-plugin/src/main/31xdb/scala']
```

* For the shimplify directory structure all files under `src/(main|test)/spark*` directories
are read to parse the `spark-rapids-shim-json-lines` comments. It performs the following
validations:

  * It makes sure that the comment is present and can be parsed
  * The list of shims is non-empty (e.g., has not orphaned through dropping shims) and sorted.
  * The file is stored under the *owner shim* directory.

* All files participating listing the `buildver` of the current Maven build session are symlinked to
`target/${buildver}/generated/src/(main|test)/(scala|java)`. Thus, instead of hardcoding distinct
lists of directories for `build-helper` Maven plugin to add (one for each shim) after the full
transition to shimplify, the pom will have only 4 add source statements that is independent of the
number of supported shims.

With the shimplify format in place it is easy to review all the files for a single Shim without
relying on Maven:

```bash
git grep '{"spark": "323"}' '*.scala' '*.java'
```

## Conversion to the Shimplify-based Directory Structure

Shimplify can automatically convert the prior version-range-with-exceptions directory structure to
the simplified version. This allows to make it an atomic transition without having to resolve
almost unavoidable merge conflicts due to the sheer size of this sweeping change while the shim
development is ongoing. The conversion of the shims source code and the regular build should not
be done simultaneously for faster isolation and correction of potential bugs in the conversion code.

Prior to invoking the conversion standalone, you first run

```bash
mvn clean install -DskipTests
```

on the current state of the `spark-rapids` repo.

After that you can execute conversion in one or more iterations depending on specified -D parameters

```bash
mvn generate-sources antrun:run@shimplify-shim-sources -Dshimplify=true [-D...]
```

With `-Dshimplify=true`, shimplify is put on the write call path to generate and inject spark-rapids-shim-json-lines comments to all shim source files. The files are not yet moved to their owner shim directory, and so it is easy to verify with `git diff` the comments being injected. If you see
any issue you can fix its cause and re-execute the command with by adding
`-Dshimplify.overwrite=true`. However, it is usually easier to just have git restore the previous state:

```bash
git restore sql-plugin tests
```


## Adding a new Shim

## Deleting a Shim

[1]: https://github.com/NVIDIA/spark-rapids/issues/3223
[2]: ../../build/shimplify.py
[3]: https://github.com/NVIDIA/spark-rapids/blob/74ce729ca1306db01359e68f7f0b7cc31cd3d850/pom.xml#L494-L500
[4]: https://jsonlines.org/