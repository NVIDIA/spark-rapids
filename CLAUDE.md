# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The RAPIDS Accelerator for Apache Spark is a plugin that leverages NVIDIA GPUs to accelerate Spark SQL processing via RAPIDS libraries. The codebase supports 10+ Spark versions (3.2.x through 4.0.x) plus vendor distributions (Databricks, Cloudera) using a sophisticated shim layer architecture.

## Build Commands

### Basic Build

```bash
# Build for a specific Spark version (required parameter)
mvn verify -Dbuildver=330              # Apache Spark 3.3.0
mvn verify -Dbuildver=350              # Apache Spark 3.5.0
mvn verify -Dbuildver=400              # Apache Spark 4.0.0

# Fast iteration build (skip tests)
mvn verify -DskipTests -Dbuildver=330

# Build only dependencies for a specific module
mvn package -pl tests -am -Dbuildver=330
```

### Multi-Version Distribution

```bash
# Use buildall script for multi-version builds
./build/buildall --profile=noSnapshots          # All released versions (no DB)
./build/buildall --profile=snapshots            # Including snapshot versions
./build/buildall --profile=330,350              # Specific versions only
./build/buildall --parallel=4 --profile=330     # Parallel build with 4 jobs

# Quick single-version build
./build/buildall --profile=330 --module=dist
```

### Scala 2.13 Build

```bash
# Build for Scala 2.13 (separate build tree in scala2.13/)
mvn -f scala2.13/ -Dbuildver=350 verify

# Sync pom changes between 2.12 and 2.13 (REQUIRED after pom modifications)
./build/make-scala-version-build-files.sh 2.13
```

### Fast Iteration

```bash
# Skip JAR compression for faster dist builds (development only)
mvn package -pl dist -PnoSnapshots -Ddist.jar.compress=false -Drapids.jni.unpack.skip

# Rebuild distribution only after changes
./build/buildall --rebuild-dist-only --option="-Ddist.jar.compress=false"
```

## Testing

### Unit Tests (Scala/ScalaTest)

```bash
# Run all unit tests
mvn package -pl tests -am -Dbuildver=330

# Run specific test suite
mvn package -pl tests -am -DwildcardSuites=com.nvidia.spark.rapids.ParquetWriterSuite

# Run tests matching pattern
mvn package -pl tests -am -Dsuffixes='.*CastOpSuite' -Dtests=decimal

# With custom Spark config
SPARK_CONF="spark.dynamicAllocation.enabled=false" mvn package -pl tests -am
```

### Integration Tests (Python/pytest)

```bash
# Run specific test file
./integration_tests/run_pyspark_from_build.sh -k map_test.py

# Run specific test within file
./integration_tests/run_pyspark_from_build.sh -k 'map_test and test_map'

# With environment variable
TESTS="arithmetic_ops_test.py::test_addition" ./integration_tests/run_pyspark_from_build.sh

# Run Delta Lake tests (requires Delta Lake jars and session extensions)
PYSP_TEST_spark_jars_packages="io.delta:delta-spark_2.12:3.3.0" \
PYSP_TEST_spark_sql_extensions="io.delta.sql.DeltaSparkSessionExtension" \
PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
integration_tests/run_pyspark_from_build.sh -m delta_lake --delta_lake

# Run specific Delta Lake test(s)
PYSP_TEST_spark_jars_packages="io.delta:delta-spark_2.12:3.3.0" \
PYSP_TEST_spark_sql_extensions="io.delta.sql.DeltaSparkSessionExtension" \
PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
integration_tests/run_pyspark_from_build.sh -m delta_lake --delta_lake -k "test_name"
```

## Architecture

### Shim Layer (Multi-Version Support)

The codebase uses a **Parallel World Classloader** pattern to support multiple incompatible Spark versions in a single JAR:

- **Compile-time shims**: Version-specific code in `sql-plugin/src/main/spark3XX/scala/`
- **Runtime shim loading**: `ShimLoader` detects Spark version and configures parallel classloader URLs
- **JAR structure**:
  ```
  rapids-4-spark.jar
  ├── com/nvidia/spark/SQLPlugin.class  (public API, no shim)
  ├── spark-shared/                     (shared across all versions)
  ├── spark330/                         (Spark 3.3.0 specific)
  ├── spark350/                         (Spark 3.5.0 specific)
  └── spark400/                         (Spark 4.0.0 specific)
  ```
- **Key insight**: Classes with identical names but version-specific implementations are isolated using JAR-internal prefixes like `spark330/com/nvidia/...`

### Module Structure

```
ROOT pom.xml
├── sql-plugin-api       Public API (no shims, loaded early by Spark)
├── sql-plugin           Core GPU execution engine (shimmed per Spark version)
├── shuffle-plugin       GPU shuffle manager (shimmed)
├── udf-compiler         UDF to GPU compilation (shimmed)
├── aggregator           Shades dependencies into single artifact per Spark version
├── dist                 Final multi-version JAR with parallel world structure
├── tests                Scala unit tests (depends on aggregator)
├── integration_tests    Python/PySpark integration tests
├── shim-deps           Spark version-specific Maven dependencies
├── delta-lake/          Delta Lake version modules (delta-20x through delta-40x)
├── iceberg/             Iceberg support modules
└── scala2.13/           Mirror build tree for Scala 2.13
```

### Key Design Patterns

**GPU Semaphore Pattern**:
- `GpuSemaphore` limits concurrent GPU tasks
- Must acquire when transitioning CPU→GPU data
- Must release when transitioning GPU→CPU data
- Critical for preventing GPU OOM

**Columnar Batch Processing**:
- Plugin operates on `ColumnarBatch` rather than row-by-row
- GPU-optimized batch format
- Key transition nodes: `GpuRowToColumnar`, `GpuColumnarToRow`

**ShimExpression Pattern**:
- Intermediate traits bridge incompatible base class changes between Spark versions
- Example: `TreeNode` methods differ between Spark 3.1.x and 3.2.x
- Solution: Create `ShimExpression` trait with version-specific implementations in `sparkXXX/` directories

**Late Binding for Public Classes**:
- Public classes (e.g., `RapidsShuffleManager`) use lazy vals to delay loading parallel world classes
- Prevents classloader issues for classes loaded before plugin initialization

### Source Code Organization

**Base code** (version-compatible):
- `sql-plugin/src/main/scala/` - Shared across all Spark versions
- `sql-plugin/src/main/scala-2.12/` - Scala 2.12 specific
- `sql-plugin/src/main/scala-2.13/` - Scala 2.13 specific

**Version-specific overrides**:
- `sql-plugin/src/main/spark330/scala/` - Spark 3.3.0 only
- `sql-plugin/src/main/spark340/scala/` - Spark 3.4.0 only
- `sql-plugin/src/main/spark350/scala/` - Spark 3.5.0 only
- `sql-plugin/src/main/spark400/scala/` - Spark 4.0.0 only

### Configuration

- **Entry point**: `com.nvidia.spark.SQLPlugin` (implements Spark's `Plugin` interface)
- **Enable plugin**: `--conf spark.plugins=com.nvidia.spark.SQLPlugin`
- **Plugin configs**: `--conf spark.rapids.*=value`
- **Config registry**: All configs defined in `RapidsConf.scala`

## Development Workflow

### Making Code Changes

1. **Determine if change is version-specific**:
   - If code works across all Spark versions → modify in `src/main/scala/`
   - If code differs by Spark version → add/modify in `src/main/sparkXXX/scala/`

2. **Handling base class incompatibilities**:
   - If Spark upstream changes base class signatures, create a shim trait in `sparkXXX/` directories
   - See `ShimExpression` pattern in `sql-plugin/src/main/sparkXXX/scala/com/nvidia/spark/rapids/shims/`

3. **After modifying pom.xml files**:
   - **REQUIRED**: Run `./build/make-scala-version-build-files.sh 2.13` to sync Scala 2.13 poms
   - This ensures Scala 2.13 build picks up dependency changes

4. **Build and test**:
   ```bash
   mvn verify -Dbuildver=330          # Build single version
   mvn package -pl tests -am          # Run unit tests
   ./integration_tests/run_pyspark_from_build.sh  # Run integration tests
   ```

### IDE Setup (IntelliJ IDEA)

1. **First-time setup**:
   ```bash
   mvn clean install -Dbuildver=330 -DskipTests -Dmaven.scaladoc.skip
   ```

2. **Import project**: Open `pom.xml` in IDEA

3. **Settings** (`File | Settings | Build Tools | Maven | Importing`):
   - Uncheck "Import using new IntelliJ Workspace Model API"
   - Uncheck "Keep source and test folders on reimport"
   - Set "Generated sources folders" to "Detect automatically"
   - Set "Phase to be used for folders update" to `process-test-resources`

4. **Select profile**: Maven tool window → select `release330` (or desired Spark version)

5. **Refresh**:
   - "Reload all projects"
   - "Generate Sources and Update Folders For all Projects"

6. **Known issue**: Test source folders in `tests/src/test/spark3XX` may need manual marking as "Test Sources Root"

### Cross-Repository Development (spark-rapids-jni)

The plugin depends on `spark-rapids-jni` (JNI layer for cuDF operations). When developing across both repositories:

1. Build and install spark-rapids-jni locally:
   ```bash
   cd spark-rapids-jni
   mvn install -DskipTests
   ```

2. Build spark-rapids with local JNI:
   ```bash
   cd spark-rapids
   mvn verify -Dbuildver=330
   ```

3. Maven will automatically use the locally installed JNI SNAPSHOT

## Important Constraints

### Scala Compatibility
- All code must compile with both Scala 2.12 and 2.13
- Test both when making changes to ensure compatibility
- Scala 2.13 support is required for Spark 3.3.0+

### Shim Loading Constraints
- Public API classes in `sql-plugin-api` must NOT reference shimmed classes directly
- Use `ShimLoader` to instantiate version-specific implementations
- Avoid early loading of shimmed classes (use lazy vals for late binding)

### Resource Management

**Always use `safeClose()` when closing a collection of `AutoCloseable` resources** — never loop
and call `.close()` manually. A plain loop stops at the first failure, leaking all remaining
resources. `safeClose()` is an implicit method (defined in `implicits.scala`) that closes every
element and suppresses/collects exceptions so none are silently swallowed:

```scala
// Wrong — a close failure leaks the rest
resources.foreach(_.close())

// Correct — closes all elements even if earlier ones throw
resources.safeClose()

// Correct — with a prior exception: adds close failures as suppressed exceptions
resources.safeClose(e)
```

The implicit is available on any `Seq[AutoCloseable]` (or `Array`, `ArrayBuffer`, etc.) via
`com.nvidia.spark.rapids.RapidsPluginImplicits._`.

### Git Submodules
- Repository uses git submodules (e.g., `thirdparty/parquet-testing`)
- After checkout or branch switch: `git submodule update --init`

### Pre-commit Hooks
- Auto-updates copyright years if `SPARK_RAPIDS_AUTO_COPYRIGHTER=ON`
- macOS users: Install GNU sed via `brew install gnu-sed` (BSD sed causes issues)

## CI and Pull Requests

### Status Checks
- **blossom-ci**: Triggered by maintainers commenting `build` on PR
- Runs `mvn verify` and unit tests for multiple Spark versions in parallel
- Add `[skip ci]` to PR title for doc-only changes
- Add `[databricks]` to PR title to test Databricks runtimes

### Commit Sign-off
- All commits must be signed off: `git commit -s`
- Adds `Signed-off-by: Your Name <email>` to commit message

## Common Gotchas

1. **Building without `-Dbuildver`**: Build will fail or use wrong default
2. **Forgetting to sync Scala 2.13 poms**: Scala 2.13 CI will fail with dependency mismatches
3. **Directly calling shimmed classes**: Will fail at runtime due to classloader isolation
4. **Modifying pom.xml in scala2.13/**: Changes will be overwritten by sync script
5. **Running single-version JAR on different Spark version**: Shim mismatch causes runtime errors
6. **Mixing compiled versions**: Run `mvn clean` when switching between different `-Dbuildver` values
