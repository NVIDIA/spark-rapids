---
layout: page
title: RAPIDS Accelerator File Cache
parent: Additional Functionality
nav_order: 9
---

# RAPIDS Accelerator File Cache

The RAPIDS Accelerator for Apache Spark provides an optional file cache which may improve
performance of Spark applications that access the same input files multiple times. It caches
portions of remote files being accessed onto the local filesystem of executors to speedup access
if that data is accessed again in the same application.

## Limitations of the File Cache

The file cache is only used by Parquet table scans that have been GPU-accelerated by the RAPIDS
Accelerator. CPU table scans or scans of data formats other than Parquet will not use the
file cache.

The file cache does not detect when the local copy of data is stale with respect to the remote
filesystem. This is only a problem when individual input files will be overwritten during the
lifetime of the application. **If this is a possibility, DO NOT enable the filecache** for that
application. The application could crash or the application output could become corrupted due to
the use of stale input data.

The file cache does not perform well if the executor node's local disks are relatively slow. The
file cache performs best when the local disks are significantly faster than the distributed
filesystem from which data is being cached. Enabling the file cache when the executor local disks
are too slow can cause applications to run slower rather than faster.

## Configuring the File Cache

File caching is disabled by default. It can be enabled by setting spark.rapids.filecache.enabled
to true. The file cache stores data locally in the same local directories that have been
configured for the Spark executor.

By default the file cache will use up to half of the available space in the Spark local
directories. To specify an absolute limit, set spark.rapids.filecache.maxBytes to the maximum
number of bytes to use for the file cache in a single executor. For example, setting
spark.rapids.filecache.maxBytes=50g will limit the filecache to 50 gigabytes of local storage per
executor.
