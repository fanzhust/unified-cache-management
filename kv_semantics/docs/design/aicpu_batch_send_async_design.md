# AICPU 跨 Send 异步 Launch/Wait 方案

## 1. 目标

实际运行中一次 `Send` 的 `groups.size()` 通常为 1，因此在单次 `Send` 内并行 connection group 不能解决主要瓶颈。目标调整为：让同一个 transport worker 连续发起多个 `Send`，把多个 `HixlBatchSend` 排入 ACL stream 后，再统一进入等待阶段。

本方案不创建 Host 线程池。所有 ACL/HIXL launch 和 wait 仍由原来的单个 transport worker 顺序调用，以规避多个 Host 线程同时操作 ACL context、stream 和 HIXL function handle 的风险。

## 2. 接口与兼容性

原同步接口和实现完整保留：

```cpp
std::vector<Status> TransProvider::Send(...);
Status AICPUTransProvider::Impl::LaunchBatchSendLocked(...);
```

新增 provider 两阶段接口：

```cpp
class TransProvider::SendOperation;
using SendOperationPtr = std::unique_ptr<SendOperation>;

std::vector<Status> AsyncSend(..., SendOperationPtr& operation);
std::vector<Status> WaitSend(SendOperationPtr& operation);
```

- `AsyncSend` 只准备参数并调用 `aclrtLaunchKernelWithConfig`，成功后返回 move-only operation。
- `WaitSend` 调用 `aclrtSynchronizeStreamWithTimeout`、读取 HIXL status array，并消费 operation。
- launch 成功只表示任务已进入 stream，不表示设备执行成功；AICPU exception 和 HIXL entry error 仍在 `WaitSend` 返回。
- 不支持异步发送的 provider 保持原同步行为；配置为 async 时 transport 初始化会明确失败。

## 3. Transport 调度时序

`aicpu_send_mode=async` 时，transport worker 从现有 SPSC queue 中最多取 `aicpu_send_max_inflight` 个任务：

```text
task A: prepare -> AsyncSend -> operation A
task B: prepare -> AsyncSend -> operation B
task C: prepare -> AsyncSend -> operation C

task A: WaitSend -> launch result/status array
task B: WaitSend -> launch result/status array
task C: WaitSend -> launch result/status array

remote completion仍由 flag buffer + CompletionLoop 处理
```

这里的并发窗口是“transport task / Send 数”，不是单次 `Send` 的 connection group 数，也不是线程数。窗口为 1 时用于功能回归；建议按 2、4、8 逐步验证吞吐和稳定性。

如果 worker 取到第一个任务时队列里暂时没有更多任务，本轮只发一个，不额外等待凑批，避免人为增加空载延迟。

## 4. Workspace 和资源生命周期

同步路径继续复用 `ConnectionRecord::mappedBatchWorkspace`。

异步路径不能复用该 workspace，因为同一 connection 上的下一个 `AsyncSend` 会覆盖前一个 kernel 尚在访问的 `io_batches` 和 `status_array`。因此每个 operation/ticket 独立持有：

- mapped batch workspace；
- `shared_ptr<ConnectionRecord>`；
- batch 数量和原始下标映射；
- 绝对 deadline。

connection 记录未完成异步 send 数量。`DeleteConnections` 在数量非零时返回 `RESOURCE_BUSY`，防止 stream/channel/thread 被提前销毁。调用方必须在释放 send/flag buffer、注销 MR 或销毁 provider 之前消费所有 operation。

## 5. Completion、取消和超时

- `CompletionLoop` 只有在 `sendReturned=true` 后才轮询远端 flag，避免 kernel 尚未 drain 时释放 buffer。
- 异步 send 尚未完成时收到 cancel，只记录 `cancelRequested`；worker 仍先调用 `WaitSend`，随后再释放 sub-batch 资源并完成取消。
- 每个 operation 使用 launch 时计算的绝对 deadline，wait 使用剩余时间，避免重复获得完整 timeout。
- 某个 operation wait 失败后仍继续 drain 同一批中其他已经 launch 的 operation。
- AICPU exception/timeout 后 connection 标记为 faulted，禁止继续异步 launch，交由连接恢复或 shutdown 处理。

## 6. 配置传递

推理进程 YAML 配置：

```yaml
asu_aicpu_send_mode: "async"
asu_aicpu_send_max_inflight: 2
```

传递链路：

```text
ucm_config_asu_aicpu.yaml
  -> AsuStore::ParseConfig
  -> AsuStore::BuildTransportConfig
  -> TransportConfig.attrs
  -> AsuTransportImpl (选择同步/异步调度)
  -> AICPUTransProvider::AsyncSend/WaitSend
```

`asu_aicpu_send_max_inflight` 必须大于 0。同步模式不使用该窗口，且始终调用原 `Send`/`LaunchBatchSendLocked`。

## 7. 验证建议

1. `send_mode=sync` 对照原有功能和性能。
2. `send_mode=async, max_inflight=1` 验证两阶段接口结果与同步路径一致。
3. 在足够并发的上层 workload 下测试 2、4、8；确认日志中的每轮 `tasks/launched` 大于 1。
4. 同时记录 launch、wait、端到端耗时和吞吐；仅看单请求延迟可能无法反映排队收益。
5. 观察 AICPU/HCCP 日志、SQ 深度、timeout 和 `507018`。若 2 路即出现设备异常，不再扩大窗口，并确认 HIXL/HCOMM 对同 stream 多 kernel 排队的约束。
6. 覆盖 launch 失败、wait 失败、cancel、timeout、shutdown 和 connection delete 的资源生命周期测试。

## 8. 已淘汰实现

- Host thread pool 并行调用完整同步 launch 函数：已淘汰。
- 仅在一次 `Send` 内对 connection group 做 fan-out/fan-in：已淘汰，因为 `groups.size()==1` 是正常主路径，无法提升多个 Send 串行提交的性能。
