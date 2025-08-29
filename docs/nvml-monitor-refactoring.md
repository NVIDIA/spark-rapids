# NVML Monitor 重构总结

本文档记录了 NVML Monitor 集成和 Stage Epoch 管理重构的详细信息。

## 🎯 完成的任务

### 1. 配置重命名
- ✅ 将所有 `spark.rapids.nvml.monitor.*` 配置改名为 `spark.rapids.monitor.nvml.*`
- ✅ 更新了 `RapidsConf.scala` 中的配置定义
- ✅ 更新了示例文档中的所有配置引用

### 2. 公共 Epoch 管理抽象
- ✅ 创建了 `StageEpochManager.scala` 公共类
- ✅ 抽象出了 stage epoch 的通用逻辑：
  - 任务追踪（Task tracking）
  - Stage 主导权确定（Dominant stage determination）
  - Epoch 计数管理（Epoch counting）
  - 调度器管理（Scheduler management）
  - 回调机制（Callback mechanism）

### 3. AsyncProfilerOnExecutor 重构
- ✅ 移除了重复的 epoch 管理代码
- ✅ 集成了 `StageEpochManager`
- ✅ 使用回调模式处理 stage 切换
- ✅ 简化了代码结构，提高了可维护性

### 4. NVMLMonitorOnExecutor 重构  
- ✅ 移除了重复的 epoch 管理代码
- ✅ 集成了 `StageEpochManager`
- ✅ 统一了与 AsyncProfiler 的 epoch 管理逻辑
- ✅ 更新了配置读取逻辑

## 📁 修改的文件

### 新增文件
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/StageEpochManager.scala`
- `examples/nvml-monitor-example.md`
- `docs/nvml-monitor-refactoring.md` (本文档)

### 修改的文件
1. **`RapidsConf.scala`**
   - 重命名所有 NVML Monitor 配置项
   - 添加了 lazy val 配置访问器

2. **`asyncProfiler.scala`** 
   - 移除了原有的 epoch 管理变量和方法
   - 集成了 `StageEpochManager`
   - 重构了 stage 切换逻辑

3. **`nvmlMonitor.scala`**
   - 更新了配置名称引用
   - 集成了 `StageEpochManager`
   - 移除了重复的 epoch 管理代码

4. **`Plugin.scala`**
   - 无修改（之前已经集成了 NVMLMonitorOnExecutor）

## 🏗 架构改进

### Before (重构前)
```
AsyncProfilerOnExecutor              NVMLMonitorOnExecutor
├── runningTasks                    ├── runningTasks  
├── stageEpochCounters             ├── stageEpochCounters
├── epochScheduler                 ├── epochScheduler
├── epochTask                      ├── epochTask
├── determineAndSwitchStageEpoch() ├── determineAndSwitchStageEpoch()
├── startEpochScheduler()          ├── startEpochScheduler()
└── stopEpochScheduler()           └── stopEpochScheduler()
```

### After (重构后)
```
                    StageEpochManager (公共层)
                    ├── runningTasks
                    ├── stageEpochCounters  
                    ├── epochScheduler
                    ├── epochTask
                    ├── checkAndSwitchStageEpoch()
                    ├── start()
                    ├── stop()
                    └── onTaskStart()
                           ↑
              ┌─────────────┴─────────────┐
              ▼                           ▼
    AsyncProfilerOnExecutor    NVMLMonitorOnExecutor
    ├── stageEpochManager     ├── stageEpochManager
    └── onStageTransition()   └── onStageTransition()
```

## ✨ 重构优势

### 1. 代码复用
- 消除了 ~150 行重复代码
- 两个监控系统现在共享相同的 epoch 管理逻辑

### 2. 一致性
- 统一的 stage 切换行为
- 统一的主导 stage 确定算法（>50% 阈值）
- 统一的调度器管理

### 3. 可维护性
- 单一职责：`StageEpochManager` 专门处理 epoch 逻辑
- 更清晰的接口：回调模式解耦了 epoch 管理和具体业务逻辑
- 更容易测试：可以独立测试 epoch 管理逻辑

### 4. 可扩展性
- 未来的监控系统可以轻松集成相同的 epoch 管理
- 可配置的主导阈值（当前为 50%）
- 灵活的回调机制

## 📊 配置变更映射

| 旧配置名 | 新配置名 |
|---------|---------|
| `spark.rapids.nvml.monitor.enabled` | `spark.rapids.monitor.nvml.enabled` |
| `spark.rapids.nvml.monitor.intervalMs` | `spark.rapids.monitor.nvml.intervalMs` |
| `spark.rapids.nvml.monitor.logFrequency` | `spark.rapids.monitor.nvml.logFrequency` |
| `spark.rapids.nvml.monitor.stageMode` | `spark.rapids.monitor.nvml.stageMode` |
| `spark.rapids.nvml.monitor.stageEpochInterval` | `spark.rapids.monitor.nvml.stageEpochInterval` |

## 🧪 测试建议

1. **基本功能测试**
   - 验证 NVML 监控在 executor 模式下正常工作
   - 验证 AsyncProfiler 在重构后正常工作

2. **Stage 模式测试**  
   - 验证 stage 切换逻辑正常工作
   - 验证两个监控系统的切换时机一致

3. **配置测试**
   - 验证新的配置名称正常工作
   - 验证旧配置名称被正确弃用

4. **压力测试**
   - 多 stage 并发场景下的 epoch 管理
   - 频繁 stage 切换场景下的性能

## 🔄 迁移指南

对于现有用户，需要更新配置：

```bash
# 旧配置
--conf spark.rapids.nvml.monitor.enabled=true

# 新配置  
--conf spark.rapids.monitor.nvml.enabled=true
```

建议在升级时同时更新所有相关配置项。

## 💡 未来改进方向

1. **可配置阈值**：允许用户自定义主导 stage 的阈值（目前固定为 50%）
2. **更多监控系统**：可以将其他监控系统（如内存监控）也集成到相同的 epoch 框架
3. **更智能的切换**：考虑 stage 的历史信息来避免频繁切换
4. **指标收集**：添加 epoch 切换的指标统计

---

本重构成功地消除了代码重复，提高了系统的一致性和可维护性，为后续的监控功能扩展奠定了良好的基础。
