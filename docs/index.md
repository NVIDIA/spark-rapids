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

As data scientists shift from using traditional analytics to leveraging AI(DL/ML) applications that 
better model complex market demands, traditional CPU-based processing can no longer keep up without 
compromising either speed or cost. The growing adoption of AI in analytics has created the need for 
a new framework to process data quickly and cost-efficiently with GPUs.

The RAPIDS Accelerator for Apache Spark combines the power of the <a href="https://github.com/rapidsai/cudf/">RAPIDS cuDF</a> library and the scale of the Spark distributed computing framework.  The RAPIDS Accelerator library also has a built-in accelerated shuffle based on <a href="https://github.com/openucx/ucx/">UCX</a> that can be configured to leverage GPU-to-GPU communication and RDMA capabilities. 

If you are a customer looking for information on how to adopt Spark RAPIDS for your Spark workloads, please go to our User Guide for more information: <a href="https://docs.nvidia.com/spark-rapids/user-guide/latest/index.html">link</a>.
