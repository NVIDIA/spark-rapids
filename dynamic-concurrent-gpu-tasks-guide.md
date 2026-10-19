# Dynamic Concurrent GPU Tasks 机制与性能验证指南

## 背景

`spark.rapids.sql.concurrentGpuTasks.dynamic` 用于根据运行时观测到的 GPU
内存需求，动态调整每张 GPU 上可并发执行的 task 数量。

该功能由
[PR #12374](https://github.com/NVIDIA/cudf-spark/pull/12374)
引入，从 cuDF for Spark（原 RAPIDS Accelerator）v25.06.0 开始提供，
默认值为 `true`。v25.04 及更早版本不包含该功能。

动态模式属于启发式自动调优，主要用于减少手工设置
`spark.rapids.sql.concurrentGpuTasks` 的工作量。对于数据分布稳定且已经完成
手工调优的任务，动态模式不保证一定比固定并发配置更快。

## 配置含义

### `spark.rapids.sql.concurrentGpuTasks`

设置每个 stage 的初始并发估计。

当 dynamic 模式开启时，该值只是起点，运行期间仍可能根据 GPU 内存指标调整。

### `spark.rapids.sql.concurrentGpuTasks.dynamic`

- `true`：根据运行时 GPU 内存观测动态调整，默认值。
- `false`：保持固定的内存和并发估计。

如果需要固定并发，应同时设置：

```text
--conf spark.rapids.sql.concurrentGpuTasks=<N>
--conf spark.rapids.sql.concurrentGpuTasks.dynamic=false
```

只设置 `concurrentGpuTasks=<N>`、但不关闭 dynamic，并不能保证并发始终为
`N`。

## 实现机制

动态模式并不是直接定期修改一个“并发 task 数”变量，而是动态估算每个 task
需要的 GPU 内存，再通过 GPU semaphore 控制多少 task 可以同时进入 GPU。

整体过程如下：

1. 为每个 stage 创建独立的 GPU 内存估算器。
2. 收集 active 和 completed task 的 GPU peak-memory footprint。
3. 使用样本的第 80 百分位作为该 stage 后续 task 的内存需求估计。
4. 样本少于 4 个时，使用默认估计补足。
5. 最多保留最近 200 个 completed-task 样本。
6. 将内存估计换算成 semaphore permits；当前 GPU 剩余 permits 足够时，task
   才能进入 GPU。
7. task 完成并释放 permits 后，等待中的 task 会根据最新内存估计重新计算
   所需 permits。

每个 permit 当前对应约 32 MiB GPU 内存。

## 初始值计算

如果显式设置：

```text
spark.rapids.sql.concurrentGpuTasks=N
```

则初始单 task 内存估计约为：

```text
GPU total memory / N
```

如果没有设置，系统根据 GPU memory 和
`spark.rapids.sql.batchSizeBytes` 计算初始值，并将初始并发限制在 1 到 4 之间。

## 为什么 dynamic 可能出现性能回退

### 1. 短 task 或延迟分配 GPU 内存

active task 会在最初约 100 ms 内从默认估计逐渐过渡到实际观测值。如果 task
在较晚阶段才分配主要 GPU 内存，早期估计可能偏低，进而允许过多 task
并发，增加 spill、OOM retry 或资源竞争。

### 2. 数据或 task 内存分布存在长尾

算法使用第 80 百分位。若少数 partition 的内存需求显著更高，这些 heavy
tasks 可能没有被充分反映在并发决策中。

### 3. 同一 stage 内不同执行阶段的内存特征不同

估算器以 stage 为单位。如果同一个 stage 前后包含内存特征差异很大的
operators，早期 task 样本可能无法准确代表后续 task。

### 4. 固定配置已经接近最优

对于长期稳定运行、数据规模和 partition 分布变化较小的任务，经过验证的
固定并发值可能比通用启发式算法更准确。

动态模式的引入 PR 也明确说明当前算法属于阶段性方案，而不是适用于所有
workload 的最终调度算法。

## 建议的 A/B 验证

建议在相同输入、集群和代码版本下至少运行以下三组测试，每组重复 3 次以上。

### A：当前 dynamic 模式

```text
--conf spark.rapids.sql.concurrentGpuTasks=<current-value>
--conf spark.rapids.sql.concurrentGpuTasks.dynamic=true
```

### B：固定并发模式

```text
--conf spark.rapids.sql.concurrentGpuTasks=<manually-tuned-value>
--conf spark.rapids.sql.concurrentGpuTasks.dynamic=false
```

### C：dynamic 模式加并发上限

从 v25.10 开始，插件提供了内部配置
`spark.rapids.sql.maxConcurrentGpuTasks`，可以在保留动态估算的同时限制最大
并发：

```text
--conf spark.rapids.sql.concurrentGpuTasks=<initial-value>
--conf spark.rapids.sql.concurrentGpuTasks.dynamic=true
--conf spark.rapids.sql.maxConcurrentGpuTasks=<upper-bound>
```

该配置属于内部配置，建议在 NVIDIA 支持团队指导下使用。它由
[PR #13359](https://github.com/NVIDIA/cudf-spark/pull/13359)
引入。

## 建议收集的指标

建议保留完整 Spark event log，并开启详细 RAPIDS metrics：

```text
--conf spark.rapids.sql.metrics.level=DEBUG
```

重点比较：

- SQL 和 stage duration
- `gpuMaxConcurrentGpuTasks`
- `gpuMaxTaskFootprint`
- `gpuSemaphoreWait`
- device、host 和 disk spill
- OOM retry 次数和耗时
- GPU utilization
- GPU underloaded 或 bubble time
- 每个 stage 的 task 数、partition size 和 task duration 分布

## 如何判断回退方向

### Dynamic 并发过高

常见表现：

- dynamic 模式下 `gpuMaxConcurrentGpuTasks` 更高；
- spill 或 OOM retry 增多；
- GPU utilization 没有对应提升；
- stage duration 变长。

建议：

- 降低手工初始并发值；
- 使用最大并发上限；
- 或关闭 dynamic，使用已经验证的固定值。

### Dynamic 并发过低

常见表现：

- GPU utilization 偏低；
- GPU underloaded/bubble time 增加；
- semaphore wait 很低；
- 没有明显 spill 或 OOM；
- 固定并发模式性能更好。

建议：

- 提高初始 `concurrentGpuTasks`；
- 如果 dynamic 仍会向下调整到较低并发，使用固定模式。

## 提交问题时建议提供的信息

为便于进一步分析，请提供：

- 完整 Spark event log；
- Spark 和 cuDF for Spark 版本；
- GPU 型号和每张 GPU 显存；
- executor cores、memory 和 GPU resource 配置；
- `concurrentGpuTasks`、`dynamic`、`batchSizeBytes` 等完整 RAPIDS 配置；
- dynamic/static 每组至少 3 次运行时间；
- stage 级 semaphore、spill、memory footprint 和 GPU utilization 指标；
- 发生回退的 SQL ID 或 stage ID。

## 版本信息

- 引入 PR：
  [#12374](https://github.com/NVIDIA/cudf-spark/pull/12374)
- 首次发布版本：
  [v25.06.0](https://github.com/NVIDIA/cudf-spark/releases/tag/v25.06.0)
- 默认值：从首次发布开始即为 `true`
- v25.04：不包含该功能
- 最大并发上限：
  [#13359](https://github.com/NVIDIA/cudf-spark/pull/13359)，从 v25.10 开始提供

