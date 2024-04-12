---
layout: page
title: Dump tool for get json object
nav_order: 12
parent: Developer Overview
---

# Dump tool for get json object

## Overview
In order to help debug the issues with the `get_json_object` function, the RAPIDS Accelerator provides a
dump tool to save debug information to try and reproduce the issues. Note, the dumped data will be masked
to protect the customer data.

## How to enable
This assumes that the RAPIDs Accelerator has already been enabled.

The `get_json_object` expression may be off by default so enable it first
```
'spark.rapids.sql.expression.GetJsonObject': 'true'
```

To enable debugging just set the path to dump the data to. Note that this
path is interpreted using the Hadoop FileSystem APIs. This means that
a path with no schema will go to the default file system.

```
'spark.rapids.sql.expression.GetJsonObject.debugPath': '/tmp/DEBUG_JSON_DUMP/'
```

This path should be a directory or someplace that we can create a directory to
store files in. Multiple files may be written out. Note that each instance of
`get_json_object` will mask the data in different ways, but the same
instance should mask the data in the same way.

You may also set the max number of rows for each file/batch. Each time a new
batch of data comes into the `get_json_object` expression a new file is written
and this controls the maximum number of rows that may be written out. 
```
'spark.rapids.sql.test.get_json_object.saveRows': '1024'
```
This config can be skipped, because default value works.

## Masking
Please note that this cannot currently be disabled.
This tool should not dump the original input data.
The goal is to find out what types of issues are showing up, and ideally 
give the RAPIDS team enough information to reproduce it.
Special/retain chars, the following chars will not be masked:
```
    ASCII chars [0, 31] including space char
    { } [ ] , : " ' :  JSON structure chars, should not mask
    \  :  escape char, should not mask
    / b f n r t u : can follow \, should not mask
    - :  used by number, should not mask
    0-9  : used by number, should not mask
    e E  : used by number, e.g.: 1.0E-3, should not mask
    u A-F a-f : used by JSON string by unicode, e.g.: \u1e2F
    true :  should not mask
    false :  should not mask
    null :  should not mask
    $ [ ] . * '  : used by path, should not mask
    ?  : json path supports although Spark does not support, adding this has no side effect
```
Above special/retain chars should not be masked, or the JSON will be invalid.  
Mask logic:  
  - Assume path only contains a-z, A-Z, '-' and [0-9]
  - For char set [a-z, A-Z] minus special/retain chars like [eE1-9], create a random one to
    one mapping to mask data. e.g.: a -> b, b -> c, ..., z -> a
  - For other chars, e.g.: Chinese chars, map to a const char 's'

## Stored Data
The dumped data is stored in a CSV file, that should be compatible with Spark,
and most other CSV readers.  CSV is a format that is not great at storing complex
data, like JSON in it, so there are likely to be some small compatibility issues.
The following shows you how to read the stored data using Spark with Scala.

Spark wants the data to be stored with no line separators, but JSON can have this.
So we replace `\r` and `\n` with a character sequences that is not likely to show up
in practice. JSON data can also conflict with CSV escape handling, especially if the
input data is not valid JSON. As such we also replace double quotes and commas just in
case.

```scala
// Replace this with the actual path to read from 
val readPath = "/data/tmp/DEBUG_JSON_DUMP"

val df = spark.read.
  schema("isLegacy boolean, path string, originalInput string, cpuOutput string, gpuOutput string").
  csv(readPath)

val strUnescape = Seq("isLegacy") ++ Seq("path", "originalInput", "cpuOutput", "gpuOutput").
  map(c => s"""replace(replace(replace(replace($c, '**CR**', '\r'), '**LF**', '\n'), '**QT**', '"'), '**COMMA**', ',') as $c""")

val data = df.selectExpr(strUnescape : _*)
```