---
layout: page
title: RAPIDS Shuffle Manager
parent: Additional Functionality
nav_order: 5
---
# RAPIDS Shuffle Manager
---
**NOTE**

The _RAPIDS Shuffle Manager_ is a beta feature!

---

The RAPIDS Shuffle Manager is an implementation of the `ShuffleManager` interface in Apache Spark
that allows custom mechanisms to exchange shuffle data. It has two components: a spillable cache, 
and a transport that can utilize Remote Direct Memory Access (RDMA) and high-bandwidth transfers 
within a node that has multiple GPUs. This is possible because the plugin utilizes 
[Unified Communication X (UCX)](https://www.openucx.org/) as its transport.

- **Spillable cache**: This store keeps GPU data close by where it was produced in device memory,
but can spill in the following cases:
  - GPU out of memory: If an allocation in the GPU failed to acquire memory, spill will get triggered
    moving GPU buffers to host to allow for the original allocation to succeed.
  - Host spill store filled: If the host memory store has reached a maximum threshold 
    (`spark.rapids.memory.host.spillStorageSize`), host buffers will be spilled to disk until
    the host spill store shrinks back below said configurable threshold.
    
  Tasks local to the producing executor will short-circuit read from the cache.

- **Transport**: Handles block transfers between executors using various means like NVLink,
PCIe, Infiniband (IB), RDMA over Converged Ethernet (RoCE) or TCP, and as configured in UCX,
in these scenarios:
  - GPU-to-GPU: Shuffle blocks that were able to fit in GPU memory.
  - Host-to-GPU and Disk-to-GPU: Shuffle blocks that spilled to host (or disk) but will be manifested 
  in the GPU in the downstream Spark task.
  
### System Setup

In order to enable the RAPIDS Shuffle Manager, UCX user-space libraries and its dependencies must 
be installed on the host and within Docker containers. A host has additional requirements, like the 
MLNX_OFED driver and `nv_peer_mem` kernel module.

#### Baremetal

1. If you have Mellanox hardware, please ensure you have the [MLNX_OFED driver](https://www.mellanox.com/products/infiniband-drivers/linux/mlnx_ofed), and the
[`nv_peer_mem` kernel module](https://www.mellanox.com/products/GPUDirect-RDMA) installed. UCX packages
   are compatible with MLNX_OFED 5.0+. Please install the latest driver available.

   With `nv_peer_mem` (GPUDirectRDMA), IB/RoCE-based transfers can perform zero-copy transfers 
   directly from GPU memory. Note that GPUDirectRDMA is known to show 
   [performance and bugs](https://docs.nvidia.com/cuda/gpudirect-rdma/index.html#supported-systems) 
   in machines that don't connect their GPUs and NICs to PCIe switches (i.e. directly to the 
   root-complex). 
   
   If you encounter issues or poor performance, GPUDirectRDMA can be controlled via the 
   UCX environment variable `UCX_IB_GPU_DIRECT_RDMA=no`, but please 
   [file a GitHub issue](https://github.com/NVIDIA/spark-rapids/issues) so we can investigate 
   further.
    
2. (skip if you followed Step 1) For setups without RoCE/Infiniband, UCX 1.9.0 packaging 
   requires RDMA packages for installation. UCX 1.10.0+ will relax these requirements for these
   types of environments, so this step should not be needed in the future.
   
   If you still want to install UCX 1.9.0 in a machine without RoCE/Infiniband hardware, please 
   build and install `rdma-core`. You can use the [Docker sample below](#ucx-minimal-dockerfile) 
   as reference.
    
3. Fetch and install the UCX package for your OS and CUDA version 
   [UCX 1.9.0](https://github.com/openucx/ucx/releases/tag/v1.9.0).
   
   UCX versions 1.9 and below require the user to install the cuda-compat package
   matching the cuda version of the UCX package (i.e. `cuda-compat-11-1`), in addition to: 
   `ibverbs-providers`, `libgomp1`, `libibverbs1`, `libnuma1`, `librdmacm1`. 
   
   For UCX versions 1.10.0+, UCX will drop the `cuda-compat` requirement, and remove explicit 
   RDMA dependencies greatly simplifying installation in some cases. Note that these dependencies 
   have been met if you followed Steps 1 or 2 above.
   
#### Docker containers

Running with UCX in containers imposes certain requirements. In a multi-GPU system, all GPUs that 
want to take advantage of PCIe peer-to-peer or NVLink need to be visible within the container. For
example, if two containers are trying to communicate and each have an isolated GPU, the link between
these GPUs will not be optimal, forcing UCX to stage buffers to the host or use TCP. 
Additionally, if you want to use RoCE/Infiniband, the `/dev/infiniband` device should be exposed 
in the container. 

If UCX will be used to communicate between containers, the IPC (`--ipc`) and 
PID namespaces (`--pid`) should also be shared. 

As of the writing of this document we have successfully tested `--privileged` containers, which
essentially turns off all isolation. We are also assuming `--network=host` is specified, allowing
the container to share the host's network. We will revise this document to include any new 
configurations as we are able to test different scenarios.

1. A system administrator should have performed Step 1 in [Baremetal](#baremetal) in the 
   host system.

2. Within the Docker container we need to install UCX and its requirements. The following is an 
   example of a Docker container that shows how to install `rdma-core` and UCX 1.9.0 with 
   `cuda-10.1` support. You can use this as a base layer for containers that your executors 
   will use.
   
   <a name="ucx-minimal-dockerfile"></a>
   
   ```
   ARG CUDA_VER=10.1
   
   # Throw away image to build rdma_core
   FROM ubuntu:18.04 as rdma_core
   
   RUN apt update
   RUN apt-get install -y dh-make git build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc
   
   RUN git clone --depth 1 --branch v33.0 https://github.com/linux-rdma/rdma-core
   RUN cd rdma-core && debian/rules binary
   
   # Now start the main container
   FROM nvidia/cuda:${CUDA_VER}-devel-ubuntu18.04
   
   COPY --from=rdma_core /*.deb /tmp/
   
   RUN apt update
   RUN apt-get install -y cuda-compat-10-1 wget udev dh-make libnuma1 libudev-dev libnl-3-dev libnl-route-3-dev python3-dev cython3
   RUN cd /tmp && wget https://github.com/openucx/ucx/releases/download/v1.9.0/ucx-v1.9.0-ubuntu18.04-mofed5.0-1.0.0.0-cuda10.1.deb
   RUN dpkg -i /tmp/*.deb && rm -rf /tmp/*.deb
   ```
  
### Validating UCX Environment

After installing UCX you can utilize `ucx_info` and `ucx_perftest` to validate the installation.

In this section, we are using a docker container built using the sample dockerfile above. 

1. Start the docker container with `--privileged` mode. In this example, we are also adding 
   `--device /dev/infiniband` to make Mellanox devices available for our test:
    ```
    nvidia-docker run \
     --network=host \
     --device /dev/infiniband \
     --privileged \
     -it \
     ucx_container:latest \
     /bin/bash 
    ```
   If you are testing between different machines, please run the above command in each node.
   
2. Test to check whether UCX can link against CUDA:
    ```
    root@test-machin:/# ucx_info -d|grep cuda     
    # Memory domain: cuda_cpy
    #     Component: cuda_cpy
    #      Transport: cuda_copy
    #         Device: cuda
    # Memory domain: cuda_ipc
    #     Component: cuda_ipc
    #      Transport: cuda_ipc
    #         Device: cuda
    ```
   
3. Mellanox device seen by UCX, and what transports are enabled (i.e. `rc`)
   ```
   root@test-machine:/# ucx_info -d|grep mlx5_3:1 -B1
   #      Transport: rc_verbs
   #         Device: mlx5_3:1
   --
   #      Transport: rc_mlx5
   #         Device: mlx5_3:1
   --
   #      Transport: dc_mlx5
   #         Device: mlx5_3:1
   --
   #      Transport: ud_verbs
   #         Device: mlx5_3:1
   --
   #      Transport: ud_mlx5
   #         Device: mlx5_3:1
   ```
   
4. You should be able to execute `ucx_perftest`, and get a good idea that things are working as
   you expect. 
   
   Example 1: GPU <-> GPU in the same host. Without NVLink you should expect PCIe speeds. In this
   case this is PCIe3, and somewhere along the lines of ~10GB/sec is expected. It should also match
   the performance seen in `p2pBandwidthLatencyTest`, which is included with the cuda toolkit.
   
   - On server container:
     ```
     root@test-server:/# CUDA_VISIBLE_DEVICES=0 ucx_perftest -t tag_bw -s 10000000 -n 1000 -m cuda
     ```
   
   - On client container:
     ```
     root@test-client:/# CUDA_VISIBLE_DEVICES=1 ucx_perftest -t tag_bw -s 10000000 -n 1000 -m cuda localhost
     +--------------+--------------+-----------------------------+---------------------+-----------------------+
     |              |              |      overhead (usec)        |   bandwidth (MB/s)  |  message rate (msg/s) |
     +--------------+--------------+---------+---------+---------+----------+----------+-----------+-----------+
     |    Stage     | # iterations | typical | average | overall |  average |  overall |  average  |  overall  |
     +--------------+--------------+---------+---------+---------+----------+----------+-----------+-----------+
     Final:                  1000     0.000   986.122   986.122     9670.96    9670.96        1014        1014
     ```
     
   Example 2: GPU <-> GPU across the network, using GPUDirectRDMA. You will notice that in this
   example we picked GPU 3. In our test machine, GPU 3 is closest (same root complex) to the NIC
   we are using for RoCE, and yields better performance than GPU 0, for example, which is sitting
   on a different socket. 
   
   - On server container:
     ```
     root@test-server: CUDA_VISIBLE_DEVICES=3 ucx_perftest -t tag_bw -s 10000000 -n 1000 -m cuda
     ```
     
   - On client container:
     ```
     root@test-client:/# CUDA_VISIBLE_DEVICES=3 ucx_perftest -t tag_bw -s 10000000 -n 1000 -m cuda test-server
     +--------------+--------------+-----------------------------+---------------------+-----------------------+
     |              |              |      overhead (usec)        |   bandwidth (MB/s)  |  message rate (msg/s) |
     +--------------+--------------+---------+---------+---------+----------+----------+-----------+-----------+
     |    Stage     | # iterations | typical | average | overall |  average |  overall |  average  |  overall  |
     +--------------+--------------+---------+---------+---------+----------+----------+-----------+-----------+
     [thread 0]               498     0.000  2016.444  2016.444     4729.49    4729.49         496         496
     [thread 0]               978     0.000  2088.412  2051.766     4566.50    4648.07         479         487
     Final:                  1000     0.000  3739.639  2088.899     2550.18    4565.44         267         479
     ```
     
   Example 3: GPU <-> GPU across the network, without GPUDirectRDMA. You will notice that the
   bandwidth achieved is higher than with GPUDirectRDMA on. This is expected, and a known issue in
   machines where GPUs and NICs are connected directly to the root complex.
   
   - On server container:
     ```
     root@test-server:/# UCX_IB_GPU_DIRECT_RDMA=no CUDA_VISIBLE_DEVICES=3 ucx_perftest -t tag_bw -s 10000000 -n 1000 -m cuda
     ```
   
   - On client container:
     ```
     root@test-client:/# UCX_IB_GPU_DIRECT_RDMA=no CUDA_VISIBLE_DEVICES=3 ucx_perftest -t tag_bw -s 10000000 -n 1000 -m cuda test-server
     +--------------+--------------+-----------------------------+---------------------+-----------------------+
     |              |              |      overhead (usec)        |   bandwidth (MB/s)  |  message rate (msg/s) |
     +--------------+--------------+---------+---------+---------+----------+----------+-----------+-----------+
     |    Stage     | # iterations | typical | average | overall |  average |  overall |  average  |  overall  |
     +--------------+--------------+---------+---------+---------+----------+----------+-----------+-----------+
     [thread 0]               670     0.000  1497.859  1497.859     6366.91    6366.91         668         668
     Final:                  1000     0.000  1718.843  1570.784     5548.35    6071.33         582         637
     ```

### Spark App Configuration

1. Choose the version of the shuffle manager that matches your Spark version. 
   Currently we support:

    | Spark Shim | spark.shuffle.manager value                              |
    | -----------| -------------------------------------------------------- |
    | 3.0.0      | com.nvidia.spark.rapids.spark300.RapidsShuffleManager    |
    | 3.0.0 EMR  | com.nvidia.spark.rapids.spark300emr.RapidsShuffleManager |
    | 3.0.1      | com.nvidia.spark.rapids.spark301.RapidsShuffleManager    |
    | 3.0.1 EMR  | com.nvidia.spark.rapids.spark301emr.RapidsShuffleManager |
    | 3.0.2      | com.nvidia.spark.rapids.spark302.RapidsShuffleManager    |
    | 3.0.3      | com.nvidia.spark.rapids.spark303.RapidsShuffleManager    |
    | 3.1.1      | com.nvidia.spark.rapids.spark311.RapidsShuffleManager    |
    | 3.1.2      | com.nvidia.spark.rapids.spark312.RapidsShuffleManager    |
    | 3.2.0      | com.nvidia.spark.rapids.spark320.RapidsShuffleManager    |

2. Recommended settings for UCX 1.9.0+
```shell
...
--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark300.RapidsShuffleManager \
--conf spark.shuffle.service.enabled=false \
--conf spark.executorEnv.UCX_TLS=cuda_copy,cuda_ipc,rc,tcp \
--conf spark.executorEnv.UCX_ERROR_SIGNALS= \
--conf spark.executorEnv.UCX_RNDV_SCHEME=put_zcopy \
--conf spark.executorEnv.UCX_MAX_RNDV_RAILS=1 \
--conf spark.executorEnv.UCX_MEMTYPE_CACHE=n \
--conf spark.executorEnv.UCX_IB_RX_QUEUE_LEN=1024 \
--conf spark.executor.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}
```

Please note `LD_LIBRARY_PATH` should optionally be set if the UCX library is installed in a
non-standard location.

#### UCX Environment Variables
- `UCX_TLS`: 
  - `cuda_copy`, and `cuda_ipc`: enables handling of CUDA memory in UCX, both for copy-based transport
    and peer-to-peer communication between GPUs (NVLink/PCIe).
  - `rc`: enables Infiniband and RoCE based transport in UCX.
  - `tcp`: allows for TCP communication in cases where UCX deems necessary.
- `UCX_ERROR_SIGNALS=`: Disables UCX signal catching, as it can cause issues with the JVM.
- `UCX_MAX_RNDV_RAILS=1`: Set this to `1` to disable multi-rail transfers in UCX, where UCX splits
  data to utilize various channels (e.g. two NICs). A value greater than `1` can cause a performance drop 
  for high-bandwidth transports between GPUs.
- `UCX_MEMTYPE_CACHE=n`: Disables a cache in UCX that can cause UCX to fail when running with CUDA buffers. 
- `UCX_RNDV_SCHEME=put_zcopy`: By default, `UCX_RNDV_SCHEME=auto` will pick different schemes for 
  the RNDV protocol (`get_zcopy` or `put_zcopy`) depending on message size, and on other parameters 
  given the hardware, transports, and settings. We have found that `UCX_RNDV_SCHEME=put_zcopy` 
  is more reliable than automatic detection, or `get_zcopy` in our testing, especially in UCX 1.9.0. 
  The main difference between get and put is the direction of transfer. A send operation under 
  `get_zcopy` will really be `RDMA READ` from the receiver, whereas the same send will be 
  `RDMA_WRITE` from the sender if `put_zcopy` is utilized.
- `UCX_IB_RX_QUEUE_LEN=1024`: Length of receive queue for the Infiniband/RoCE transports. The 
  length change is recommended as it has shown better performance when there is memory pressure 
  and message sizes are relatively large (> few hundred Bytes)
  
### Fine Tuning
Here are some settings that could be utilized to fine tune the _RAPIDS Shuffle Manager_:

#### Bounce Buffers
The following configs control the number of bounce buffers, and the size. Please note that for
device buffers, two pools are created (for sending and receiving). Take this into account when
sizing your pools.

The GPU buffers should be smaller than the [`PCI BAR
Size`](https://docs.nvidia.com/cuda/gpudirect-rdma/index.html#bar-sizes) for your GPU. Please verify
the [defaults](../configs.md) work in your case.

- `spark.rapids.shuffle.ucx.bounceBuffers.device.count`
- `spark.rapids.shuffle.ucx.bounceBuffers.host.count`
- `spark.rapids.shuffle.ucx.bounceBuffers.size`

#### Spillable Store
This setting controls the amount of host memory (RAM) that can be utilized to spill GPU blocks when
the GPU is out of memory, before going to disk. Please verify the [defaults](../configs.md).
- `spark.rapids.memory.host.spillStorageSize`

##### Shuffle Garbage Collection
Shuffle buffers cached in the spillable store, whether they are in the GPU, host, or disk, will not
be removed even after all actions for your query complete. This is a design decision in Spark, where 
shuffle temporary stores are cleaned when there is a garbage collection on the driver, and the 
references to the RDDs supporting your query are not reachable. 

One of the issues with this is with large JVM footprints in the driver. The driver may not run a GC at
all between different parts of your application, causing output for shuffle to accumulate (output that
will not be reused), and eventually causing OOM or even filled disk. This is true for Spark even without
the RAPIDS Shuffle Manager, but in our case it's likely GPU memory that is being occupied, and performance
degrades given the churn due to spill to host memory or disk. As of this stage, there isn't a good solution 
for this, other than to trigger a GC cycle on the driver.

Spark has a configuration `spark.cleaner.periodicGC.interval` (defaults to 30 minutes), that 
can be used to periodically cause garbage collection. If you are experiencing OOM situations, or 
performance degradation with several Spark actions, consider tuning this setting in your jobs.
