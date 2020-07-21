---
layout: page
title: Frequently Asked Questions
nav_order: 8
---
# Frequently Asked Questions

### Why does `explain()` show that the GPU will be used even after setting `spark.rapids.sql.enabled` to `false`?

Apache Spark caches what is used to build the output of the `explain()` function. That cache has no
knowledge about configs, so it may return results that are not up to date with the current config
settings. This is true of all configs in Spark. If you changed
`spark.sql.autoBroadcastJoinThreshold` after running `explain()` on a `DataFrame`, the resulting
query would not change to reflect that config and still show a `SortMergeJoin` even though the
new config might have changed to be a `BroadcastHashJoin` instead. When actually running something
like with `collect`, `show` or `write` a new `DataFrame` is constructed causing spark to replan the
query. This is why `spark.rapids.sql.enabled` is still respected when running, even if explain
shows stale results.

