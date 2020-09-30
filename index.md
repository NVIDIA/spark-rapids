---
layout: default
title: Home
nav_order: 1
permalink: /
description: This site serves as a collection of documentation about the RAPIDS accelerator for Apache Spark
---
# Overview
The RAPIDS Accelerator for Apache Spark leverages GPUs to accelerate processing via the
[RAPIDS libraries](http://rapids.ai).

As data scientists shift from using traditional analytics to leveraging AI applications that better model complex market demands, traditional CPU-based processing can no longer keep up without compromising either speed or cost. The growing adoption of AI in analytics has created the need for a new framework to process data quickly and cost efficiently with GPUs.

The RAPIDS Accelerator for Apache Spark combines the power of the <a href="https://github.com/rapidsai/cudf/">RAPIDS cuDF</a> library and the scale of the Spark distributed computing framework.  The RAPIDS Accelerator library also has a built-in accelerated shuffle based on <a href="https://github.com/openucx/ucx/">UCX</a> that can be configured to leverage GPU-to-GPU communication and RDMA capabilities. 

## Performance & Cost Benefits
Rapids Accelerator for Apache Spark reaps the benefit of GPU performance while saving infrastructure costs.
![Perf-cost](/docs/img/perf-cost.png)
*ETL for FannieMae Mortgage Dataset (~200GB) as shown in our [demo](https://databricks.com/session_na20/deep-dive-into-gpu-support-in-apache-spark-3-x). Costs based on Cloud T4 GPU instance market price & V100 GPU price on Databricks Standard edition


## Ease of Use
Run your existing Apache Spark applications with no code change.  Launch Spark with the RAPIDS Accelerator for Apache Spark plugin jar and enable a configuration setting: 

`spark.conf.set('spark.rapids.sql.enabled','true')`

The following is an example of a physical plan with operators running on the GPU: 

![ease-of-use](/docs/img/ease-of-use.png)

Learn more on how to [get started](get-started/getting-started.md).

## A Unified AI framework for ETL + ML/DL 
A single pipeline, from ingest to data preparation to model training
![spark3cluster](/docs/img/spark3cluster.png)


