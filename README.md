# RAPIDS Accelerator For Apache Spark
NOTE: For the latest stable [README.md](https://github.com/nvidia/spark-rapids/blob/main/README.md) ensure you are on the main branch. The RAPIDS Accelerator for Apache Spark provides a set of plugins for Apache Spark that leverage GPUs to accelerate processing via the RAPIDS libraries and UCX. Documentation on the current release can be found [here](https://nvidia.github.io/spark-rapids/).

The RAPIDS Accelerator for Apache Spark provides a set of plugins for
[Apache Spark](https://spark.apache.org) that leverage GPUs to accelerate processing
via the [RAPIDS](https://rapids.ai) libraries and [UCX](https://www.openucx.org/).

To get started and try the plugin out use the [getting started guide](./docs/get-started/getting-started.md).

## Compatibility

The SQL plugin tries to produce results that are bit for bit identical with Apache Spark.
Operator compatibility is documented [here](./docs/compatibility.md)

## Tuning

To get started tuning your job and get the most performance out of it please start with the
[tuning guide](./docs/tuning-guide.md).

## Configuration

The plugin has a set of Spark configs that control its behavior and are documented
[here](docs/configs.md).

## Issues & Questions

We use github to track bugs, feature requests, and answer questions. File an
[issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose) for a bug or feature request. Ask
or answer a question on the [discussion board](https://github.com/NVIDIA/spark-rapids/discussions).

## Download

The jar files for the most recent release can be retrieved from the [download](docs/download.md)
page.

## Building From Source

See the [build instructions in the contributing guide](CONTRIBUTING.md#building-from-source).

## Testing

Tests are described [here](tests/README.md).

## Integration
The RAPIDS Accelerator For Apache Spark does provide some APIs for doing zero copy data
transfer into other GPU enabled applications.  It is described
[here](docs/additional-functionality/ml-integration.md).

Currently, we are working with XGBoost to try to provide this integration out of the box.

You may need to disable RMM caching when exporting data to an ML library as that library
will likely want to use all of the GPU's memory and if it is not aware of RMM it will not have
access to any of the memory that RMM is holding.

## Qualification and Profiling tools

The Qualification tool is used to look at a set of applications to determine if the RAPIDS Accelerator for Apache Spark
might be a good fit for those applications.

The Profiling tool generates information which can be used for debugging and profiling applications.
Information such as Spark version, executor information, properties and so on. This runs on either CPU or
GPU generated event logs.

Please refer to [Qualification tool documentation](docs/spark-qualification-tool.md)
and [Profiling tool documentation](docs/spark-profiling-tool.md)
for more details on how to use the tools.

## Dependency for External Projects

If you need to develop some functionality on top of RAPIDS Accelerator For Apache Spark (we currently
limit support to GPU-accelerated UDFs) we recommend you declare our distribution artifact
as a `provided` dependency.

```xml
<dependency>
    <groupId>com.nvidia</groupId>
    <artifactId>rapids-4-spark_2.12</artifactId>
    <version>22.06.0-SNAPSHOT</version>
    <scope>provided</scope>
</dependency>
```
