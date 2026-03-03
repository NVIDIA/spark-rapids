# RAPIDS Accelerator for Apache Spark Iceberg Support

The Iceberg support is organized into multiple Maven projects, one per Iceberg minor
version that is supported. This allows each submodule to build against the Iceberg minor
version it supports.

# Iceberg Submodules

The following table shows the mapping of Iceberg versions to their supported Spark version
and the directory that contains the corresponding support code.

| Iceberg Version | Spark Version   | Directory        |
|-----------------|-----------------|------------------|
| 1.6.x           | Spark 3.5.0-3.5.3 | `iceberg-1-6-x` |
| 1.9.x           | Spark 3.5.4-3.5.7 | `iceberg-1-9-x` |

Iceberg GPU acceleration is currently only supported on Spark 3.5.x.

## Code Shared Between Modules

The `common` directory contains code that is shared across some or all of the Iceberg
submodules. It is not built directly as a Maven submodule but simply houses common code
that is picked up by the Iceberg submodules via the Maven build helper plugin.

| Directory                         | Description                              |
|-----------------------------------|------------------------------------------|
| `common/src/main/scala`           | Scala code shared across all versions    |
| `common/src/main/java`            | Java code shared across all versions     |
