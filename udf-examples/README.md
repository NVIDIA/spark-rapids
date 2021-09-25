# RAPIDS Accelerated UDF Examples

This project contains sample implementations of RAPIDS accelerated
user-defined functions. See the
[RAPIDS accelerated UDF documentation](../docs/additional-functionality/rapids-udfs.md) for details
on how RAPIDS accelerated UDFs work and guidelines for creating them.

## Building the Native Code Examples

Some of the UDF examples use native code in their implementation.
Building the native code requires a libcudf build environment, so these
examples do not build by default. The `udf-native-examples` Maven profile
can be used to include the native UDF examples in the build, i.e.: specify
 `-Pudf-native-examples` on the `mvn` command-line.

## Creating a libcudf Build Environment

The `Dockerfile` in this directory can be used to setup a Docker image that
provides a libcudf build environment. This repository will either need to be
cloned or mounted into a container using that Docker image.
The `Dockerfile` contains build arguments to control the Linux version,
CUDA version, and other settings. See the top of the `Dockerfile` for details.
