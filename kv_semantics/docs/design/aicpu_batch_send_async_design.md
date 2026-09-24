# AICPU Batch Send 异步 Launch 与等待方案

## 1. 决策与范围

放弃“Host 线程池并行调用完整 `LaunchBatchSendLocked`”的方案。新方案不创建额外的
Host worker，而是将当前同步函数拆分成两个阶段：

1. `AsyncLaunchBatchSendLocked`：准备参数并把 `HixlBatchSend` 异步下发到 connection
   对应的 ACL stream，不等待 Kernel 完成。
2. `WaitBatchSend`：等待已下发任务完成，读取 HIXL `status_array`，并结束该次
   in-flight operation。

第一阶段只改造 AICPU Provider 内部实现。`TransProvider::Send` 的公共签名和同步语义保持
不变：它按可配置的 in-flight 窗口依次 launch connection group，窗口满时等待最早的
operation，最后 drain 全部 ticket，并按输入顺序返回状态。窗口为 0 或不小于 group 数量时，
退化为“先 launch 全部、再等待全部”。

本方案不把远端 KV 完成纳入 `WaitBatchSend`。远端完成仍由现有 flag buffer 和
`CompletionLoop` 处理。

## 2. 当前同步边界

当前 `LaunchBatchSendLocked` 同时完成以下工作：

```text
ensure stream/workspace
  -> fill io_batches/status_array
  -> load HIXL function
  -> build kernel args
  -> aclrtLaunchKernelWithConfig
  -> aclrtSynchronizeStreamWithTimeout
  -> read status_array
```

`aclrtLaunchKernelWithConfig` 本身是异步下发。真正使函数同步的是紧随其后的
`aclrtSynchronizeStreamWithTimeout`。

## 3. 目标时序

`Send` 中的目标时序如下：

```text
bind Provider ACL context once

group A: AsyncLaunch -> ticket A
group B: AsyncLaunch -> ticket B
window full: Wait(ticket A) -> group A statuses
group C: AsyncLaunch -> ticket C

drain: Wait(ticket B) -> group B statuses
drain: Wait(ticket C) -> group C statuses

merge statuses -> Send return
```

Host launch 仍按顺序调用，不存在多个 Host 线程同时操作共享 `aclrtFuncHandle`、ACL context
或 HIXL launch API。由于不同 connection 使用不同 stream，已经下发的 Kernel 仍可能在设备
侧重叠执行。

## 4. 建议接口

接口保持为 `Impl` 内部能力，不立即扩展 `TransProvider` 公共抽象：

```cpp
struct BatchSendTicket {
    std::shared_ptr<ConnectionRecord> connection;
    std::uint64_t sequence{0};
    std::size_t batchSize{0};
    std::chrono::steady_clock::time_point deadline;
    bool valid{false};
};

Status AsyncLaunchBatchSendLocked(
    const std::shared_ptr<ConnectionRecord>& connection,
    const std::vector<TransProvider::SendIoBatch>& ioBatches,
    std::chrono::steady_clock::time_point deadline,
    BatchSendTicket& ticket);

Status WaitBatchSend(BatchSendTicket& ticket,
                     std::vector<std::uint32_t>& hixlStatuses);
```

约定：

- `AsyncLaunchBatchSendLocked` 的调用方已持有 `connection->sendMu`。
- `BatchSendTicket` 为 move-only，一张 ticket 只能由一个 waiter 消费。
- launch 失败时不产生有效 ticket，并恢复 connection 的 in-flight 状态。
- launch 成功只表示任务已进入 stream，不表示 HIXL 执行成功。
- `WaitBatchSend` 必须完成 stream 同步并读取 `status_array` 后才能返回成功。

可以保留一个同步包装，便于单 connection 路径和回归对照。同步包装自身负责锁定 launch
阶段，并且必须在 wait 前释放 `sendMu`：

```cpp
Status SyncLaunchBatchSend(
    const std::shared_ptr<ConnectionRecord>& connection,
    const std::vector<TransProvider::SendIoBatch>& ioBatches,
    std::vector<std::uint32_t>& hixlStatuses)
{
    BatchSendTicket ticket;
    {
        std::lock_guard<std::mutex> lock(connection->sendMu);
        auto status = AsyncLaunchBatchSendLocked(connection, ioBatches, deadline, ticket);
        if (!status.ok()) { return status; }
    }
    return WaitBatchSend(ticket, hixlStatuses);
}
```

## 5. Ticket 与资源生命周期

异步 launch 返回后，AICPU Kernel 仍可能读取或写入以下资源：

- `ConnectionRecord::stream`
- HCOMM channel 和 thread
- mapped batch workspace 中的 `io_batches`
- mapped batch workspace 中的 `status_array`
- send buffer 描述符及其指向的数据
- HIXL binary/function

因此 ticket 必须至少持有 `std::shared_ptr<ConnectionRecord>`，保证 record 对象不会提前释放。
但 shared ownership 只保护 C++ 对象，不能阻止 `DeleteConnections` 提前销毁 record 内部的
stream、channel、thread 和 workspace，所以还需要显式的 in-flight 状态。

第一阶段每个 connection 只允许一个未完成的 batch send，并用显式状态区分正常执行和
失败任务：

```cpp
enum class SendFlightState {
    IDLE,
    LAUNCHED,
    WAITING,
    FAULTED,
};

struct ConnectionRecord {
    // Existing connection resources...
    std::mutex sendMu;
    SendFlightState sendState{SendFlightState::IDLE};
    std::uint64_t sendSequence{0};
    MappedBatchWorkspace mappedBatchWorkspace;
    aclrtStream stream{nullptr};
};
```

`AsyncLaunchBatchSendLocked` 在 launch 前检查 `IDLE` 并在成功下发后设置 `LAUNCHED`；
`WaitBatchSend` 开始等待时切换为 `WAITING`，成功完成后恢复 `IDLE`。Kernel exception 或
timeout 进入 `FAULTED`，禁止同一 connection 再次 launch；后续由连接清理/恢复流程处理。
当前公共 `Send` 在整个 launch/wait 周期持有 Provider 的 `resourceMu`，因此 connection 删除、
MR 更新和 Provider 资源清理不会与 in-flight kernel 交叉。

不能依赖“把 `sendMu` 从 launch 一直锁到未来的 wait”来保护资源：未来 wait 可能迁移到其他
线程，而标准 mutex 必须由获得所有权的线程释放。正确做法是显式 operation 状态或 workspace
池。

若后续需要同一个 connection 上存在多个 in-flight send，应将 workspace 改为 operation-owned
对象或 connection-local workspace pool；不能继续复用当前唯一的
`mappedBatchWorkspace`。

## 6. Wait 方案

### 6.1 第一阶段：Stream synchronize

第一阶段继续使用已经验证过的：

```cpp
aclrtSynchronizeStreamWithTimeout(connection.stream, remainingTimeoutMs);
```

但它从 launch 函数中移到 `WaitBatchSend`。等待 ticket A 时，B/C 所在 stream 已经完成下发，
可以同时运行，因此逐个 wait 不会把设备执行重新串行化。

一次 `Send` 应使用共同的绝对 deadline，而不是给每张 ticket 重新计算一个完整 timeout。否则
N 个异常 stream 最坏可能累计等待 N 倍 timeout。

### 6.2 后续阶段：ACL event/poll

若未来要让 `TransProvider::Send` 本身也返回异步 ticket，可以在每次 Kernel launch 后向同一
stream 记录 ACL event，由 completion 线程轮询或等待 event。采用 event 前必须验证：

- event 完成是否能够可靠暴露此前 AICPU Kernel exception；
- event 完成后的 Host mapped status 可见性；
- timeout/cancel 后 event、stream 和 workspace 的释放规则。

在这些语义确认前，第一阶段不引入 event，也不改变上层状态机。

## 7. `Send` 的 fan-out/fan-in 改造

建议流程：

```cpp
ValidateAllInputs();
GroupByConnection();
BindProviderAclContext();

for (group : groups) {
    lock(group.connection->sendMu);
    launchStatus = AsyncLaunchBatchSendLocked(..., ticket);
    unlock();

    if (launchStatus.ok()) {
        operations.push_back({groupIndex, std::move(ticket)});
    } else {
        SetWholeGroupStatus(groupIndex, launchStatus);
    }
}

for (operation : operations) {
    waitStatus = WaitBatchSend(operation.ticket, hixlStatuses);
    MergeGroupStatuses(operation.groupIndex, waitStatus, hixlStatuses);
}

return results;
```

所有已成功 launch 的 ticket 都必须被 wait/drain。即使其中一个 group 失败，也不能提前返回，
否则仍在运行的 Kernel 会继续访问 workspace、stream 和 HCOMM 资源。

## 8. 错误与连接状态

错误分为两个阶段：

- launch 阶段错误：ACL context、workspace、HIXL load、kernel args 或 kernel launch 失败。
- wait 阶段错误：stream timeout、AICPU exception、HIXL entry status 非零或 status 数量异常。

`aclrtLaunchKernelWithConfig` 返回成功后仍可能在 wait 阶段出现 `507018`。因此调用方不能把
launch 成功当成 send 成功。

发生 AICPU exception、HCOMM device error 或无法确认 Kernel 是否停止的 timeout 时，不应立即
把 connection workspace 标记为可复用。建议把 connection 标记为 `FAULTED/DRAINING`，禁止新
launch，并交由 connection recovery 或 shutdown 流程处理。当前错误后继续调用
`HcommChannelUpdateStagedLocalMemInfo`/`HcommChannelDestroy` 可能产生次生错误，首个 AICPU
异常仍是主要故障。

## 9. 与已观察到的并发异常的关系

新方案消除了线程池方案中的“多个 Host 线程同时调用 ACL/HIXL launch API”，因此如果之前的
`507018` 来自共享 context/function handle 的 Host 多线程竞争，新方案可能规避该问题。

但新方案仍会让多个不同 stream 上的 `HixlBatchSend` 在设备侧重叠。如果 HIXL/HCOMM AICPU
实现本身不可重入或存在并发上限，异常仍会出现。异步接口本身不能解决设备侧不可并发问题。

建议增加内部最大 device in-flight 窗口，按以下档位验证：

```text
1 -> 2 -> 4 -> 8 -> 16
```

窗口为 1 时用于功能回归；窗口大于 1 时验证 HIXL/HCOMM 实际支持的并发度。不能在没有验证的
情况下默认开放 16 路设备并发。

## 10. 析构与资源变更

以下操作在释放或修改 connection 资源前必须等待对应 in-flight operation 到达终态：

- `DeleteConnections`
- `ResetConnectionSendResources`
- Provider 析构和 HIXL binary unload
- 可能影响 channel MR 表的 memory register/unregister

建议清理顺序：

```text
stop accepting launches
  -> wait/drain all valid tickets
  -> quarantine unresolved timeout operations
  -> reset stream/workspace
  -> destroy HCOMM channel/thread
  -> unload HIXL binary
  -> destroy endpoint
```

## 11. 分阶段实施建议

### 阶段 A：拆分接口

- 新增 `AsyncLaunchBatchSendLocked` 和 `WaitBatchSend`。
- 保持公共 `TransProvider::Send` 为同步接口。
- device in-flight 窗口先使用 1 做功能回归。

### 阶段 B：单线程 fan-out/fan-in

- `Send` 依次 launch 多个 connection group，再依次 wait。
- 不引入线程池和 future。
- 先开放并发度 2，确认不同 stream/thread/channel/workspace 均独立。

### 阶段 C：稳定性和性能验证

- 测试 device in-flight 为 1/2/4/8/16。
- 记录 launch 时间、wait 时间、`Send` 总时间和端到端完成时间。
- 收集 Host plog 与 Device AICPU/HCCP 日志。
- 若 2 路即出现 `507018`，停止扩大并发并确认 HIXL/HCOMM 的可重入约束。

### 阶段 D：可选的公共异步接口

仅当上层确实需要 `TransProvider::Send` 非阻塞返回时，再引入公共 ticket/event 和 completion
状态机。该阶段需要同步改造 cancel、timeout、memory unregister、connection recovery 和
Provider shutdown，不应与内部 launch/wait 拆分混为一次修改。

## 12. 必测场景

- 单 connection：同步包装与原实现结果一致。
- 多 connection：窗口未满时连续 launch；窗口满时 wait 最早的 ticket 后继续 launch。
- 不同 connection 的 stream/thread/channel/workspace 地址互不相同。
- launch 失败后 connection 不残留错误 in-flight 状态。
- 某个 wait 失败时仍 drain 其他已 launch operation。
- HIXL entry status 能正确映射回原始 batch index。
- timeout 后 connection 不会被立即复用。
- DeleteConnections 与 in-flight send 不发生资源提前释放。
- Provider 析构会先 drain，再 unload HIXL 和销毁 HCOMM 资源。
- device in-flight 1/2/4/8/16 的稳定性、延迟和吞吐对比。

## 13. 第一版实现状态（2026-09-24）

- 已移除方案 1 的 Host thread pool、worker 配置和对应测试代码。
- 已实现 move-only `BatchSendTicket`、`AsyncLaunchBatchSendLocked` 和 `WaitBatchSend`。
- `Send` 使用单个 Host 调用线程执行滑动窗口 fan-out/fan-in；任一 wait 失败后仍继续 drain
  其他已成功 launch 的 ticket。
- `transport.aicpu_send_max_inflight` 控制设备侧最大 in-flight 数：默认值为 2，值 1 用于同步
  基线，值 0 表示不限制（先 launch 全部 group）。
- 所有 ticket 共用一次 `Send` 的绝对 deadline，避免异常情况下累计等待 N 倍 timeout。
- `Send` 在整个 launch/wait 周期持有 `resourceMu`；ticket 持有 connection 的 shared ownership；
  每个 connection 通过 `IDLE/LAUNCHED/WAITING/FAULTED` 状态阻止 workspace 和 stream 被错误复用。
- 增加 `[SendTrace] submit_complete`、`sync_begin/sync_end` 和 `fanin_complete` 日志，便于区分
  提交、等待尾部和总耗时。
- Provider 启动签名更新为 `UCM_ASU_AICPU_PROVIDER_UBC_CTP_UBG_HCOMM_HIXL_ASYNC_V11`，
  同时打印 `send_mode=async_launch_wait` 和实际配置的 `send_max_inflight`，用于确认新 SO 已生效。
