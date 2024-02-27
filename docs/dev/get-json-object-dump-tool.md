---
layout: page
title: Dump tool for get json object
nav_order: 12
parent: Developer Overview
---

# Dump tool for get json object

## Overview
In order to track the issues caused by get json object function, RAPIDS Accelerator provides this
dump tool to save the JSON and PATH to reproduce the issues. Note, the dumped data will be masked
to protect the customer data.

## How to enable
Set the following configs:
- enable GetJsonObject
```
'spark.rapids.sql.expression.GetJsonObject': 'true'
```
- enable verify GetJsonObject
```
'spark.rapids.sql.test.get_json_object': 'true'
```

- set diff save path
e,g,:
```
'spark.rapids.sql.test.get_json_object.savePath': '/tmp/get-json-object_diffs_'
```
CSV file path is /tmp/get-json-object_diffs_yyyyMMdd.csv, yyyyMMdd is current
date. Can skip this config, default value works, default value is: /tmp/get-json-object_diffs_.
Currently only supports local path.

- set max rows for each block in GetJsonObject operator when saving diffs.
```
'spark.rapids.sql.test.get_json_object.saveRows': '1024'
```
This config can be skipped, because default value works.
This config can not control total rows, it only takes effective in GetJsonObject operator.

## how to do mask
It's enabled by default, and can not be disabled.
RAPIDS Accelerator should not dump the original Customer data.
This dump tool only care about the functionality of get-json-object, the masked data should
reproduce issues if original data/path can reproduce issues. The mask is to find a way to
mask data and reproduce issues by using masked data.
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
