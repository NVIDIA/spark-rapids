---
layout: page
title: Databricks Support Matrix
nav_order: 4
---

# Databricks Support Matrix

This page summarizes the Databricks runtime combinations supported by the
current RAPIDS Accelerator release. Use it together with the
[Download](./download.md) page to select the correct plugin artifact before
deploying to a Databricks cluster.

## Runtime Compatibility

The following matrix applies to RAPIDS Accelerator for Apache Spark v26.06.0.
Databricks runtime images provide the JVM and system libraries for each row; use
the runtime-provided JDK unless a Databricks support note explicitly instructs
otherwise.

| RAPIDS Accelerator | Databricks Runtime | Apache Spark | Scala | JDK runtime | CUDA jar variants | Minimum NVIDIA driver | Notes |
|--------------------|--------------------|--------------|-------|-------------|-------------------|-----------------------|-------|
| v26.06.0 | 13.3 ML LTS GPU | 3.4.1 | 2.12 | Databricks runtime default | CUDA 12, CUDA 13 | R525+ | Supported Databricks 13.3 runtime line. |
| v26.06.0 | 14.3 ML LTS GPU | 3.5.0 | 2.12 | Databricks runtime default | CUDA 12, CUDA 13 | R525+ | Supported Databricks 14.3 runtime line. |
| v26.06.0 | 17.3 ML LTS GPU | 4.0.0 | 2.13 | Databricks runtime default | CUDA 12, CUDA 13 | R525+ | Spark 4 / Scala 2.13 Databricks runtime line. |

The v26.06.0 download page publishes Scala 2.12 and Scala 2.13 artifacts for
both CUDA 12 and CUDA 13. Use the Scala artifact that matches the Databricks
runtime's Spark/Scala line. The CUDA classifier controls which RAPIDS native
libraries are bundled in the plugin jar; it does not change the Spark or Scala
compatibility of the artifact.

## Delta Lake GPU Support on Databricks

Databricks runtimes use Databricks-specific Delta Lake implementations. The
following table summarizes Delta Lake GPU support for the Databricks runtime
lines with feature-specific coverage in the current release. `GPU` means the
operation is expected to run on the GPU when the rest of the query plan is also
GPU-compatible. `CPU fallback` means the RAPIDS Accelerator leaves that Delta
operation on the CPU for that runtime.

| Delta feature | DBR 14.3 | DBR 17.3 |
|---------------|----------|----------|
| Reads without deletion vectors | GPU | GPU |
| Deletion vector reads | CPU fallback | GPU only with metadata row index and RAPIDS deletion-vector predicate pushdown |
| Delta writes | GPU for append, overwrite, CTAS, and RTAS | GPU for append and overwrite. CTAS and RTAS fall back to CPU. |
| Delta writes with deletion vectors | CPU fallback | CPU fallback |
| DELETE and UPDATE | GPU for copy-on-write. Operations that write deletion vectors fall back to CPU. | Same as DBR 14.3. Liquid-clustered paths also fall back to CPU. |
| MERGE | GPU, including liquid clustering | GPU for non-liquid-clustered tables only. Liquid-clustered and persistent deletion-vector writes fall back to CPU. |
| OPTIMIZE | CPU fallback | GPU for standard deletion-vector-free, non-liquid-clustered tables only |
| Auto compaction | GPU when triggered by supported GPU writes | GPU for inline deletion-vector-free, non-liquid-clustered tables only |
| Liquid clustering | GPU support | CPU fallback |

## Compatibility Caveats

- Databricks may patch existing runtime versions without changing the public
  runtime line. Use a RAPIDS Accelerator release that explicitly lists the
  Databricks runtime you plan to run.
- Runtime patch changes can surface as binary compatibility failures such as
  `NoSuchMethodError` against Spark or Databricks-internal classes. If this
  happens, first verify the RAPIDS Accelerator release and Databricks runtime
  combination against this page and the release notes.
- Earlier v26.04.0 artifacts on Databricks 17.3 could encounter a
  `NoSuchMethodError` on `CatalogTable.copy` in
  `GpuCreateDataSourceTableAsSelectCommand`; use v26.04.1 or later for that
  runtime line.
- Delta feature support is operation-specific. Even when a runtime is listed as
  supported, individual Delta reads, writes, DML, `OPTIMIZE`, auto compaction,
  deletion-vector, or liquid-clustering paths may still fall back to CPU as
  shown above.
- `spark.rapids.sql.explain=NOT_ON_GPU` can be used to confirm whether a
  particular query plan stayed on the GPU or fell back to CPU.
