---
layout: page
title: The Local Replay Framework
nav_order: 13
parent: Developer Overview
---

# Local Replay Framework

## Overview

LORE (the local replay framework) is a tool that allows developer to replay the execution of a 
gpu operator in local environment, so that developer could debug and profile the operator for 
performance analysis. In high level it works as follows:

1. Each gpu operator will be assigned a LORE id, which is a unique identifier for the operator. 
   This id is guaranteed to be unique within the same query, and guaranteed to be same when two 
   sql executions have same sql, same configuration, and same data. 
2. In the first run of the query, developer could found the LORE id of the operator they are 
   interested in by checking spark ui, where LORE id usually appears in the arguments of operator.
3. In the second run of the query, developer needs to configure the LORE ids of the operators they 
   are interested in, and LORE will dump the input data of the operator to given path.
4. Developer could copy the dumped data to local environment, and replay the operator in local 
   environment.

## Configuration

By default, LORE id will always be generated for operators, but user could disable this behavior 
by setting `spark.rapids.sql.lore.tag.enabled` to `false`. 

To tell LORE the LORE ids of the operators you are interested in, you need to set 
`spark.rapids.sql.lore.idsToDump`. For example, you could set it to "1[*], 2[*], 3[*]" to tell 
LORE to dump all partitions of input data of operators with id 1, 2, or 3. You can also only dump 
some partition of the operator's input by appending partition numbers to lore ids. For example, 
"1[0 4-6 7], 2[*]" tell LORE to dump operator with LORE id 1, but only dump partition 0, 4, 5, 6, 
and 7. But for operator with LORE id 2, it will dump all partitions. 

You also need to set `spark.rapids.sql.lore.dumpPath` to tell LORE where to dump the data, the 
value of which should point to a directory. All dumped data of a query will live in this 
directory. A typical directory hierarchy would look like this:

```console
+ loreId-10/
  - plan.meta
  + input-0/
    - rdd.meta
    + partition-0/
      - partition.meta
      - batch-0.parquet
      - batch-1.parquet
    + partition-1/
      - partition.meta
      - batch-0.parquet
  + input-1/
    - rdd.meta
    + partition-0/
      - partition.meta
      - batch-0.parquet
      - batch-1.parquet
 
+ loreId-15/
  - plan.meta
  + input-0/
    - rdd.meta
    + partition-0/
      - partition.meta
      - batch-0.parquet
```


