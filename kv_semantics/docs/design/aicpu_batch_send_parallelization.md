# AICPU Batch Send 并发化方案

## 1. 背景

`AICPUTransProvider::Send` 会先将 `SendIoBatch` 按 `ConnectionRecord` 分组，然后依次调用
`LaunchBatchSendLocked` 。当前 `LaunchBatchSendLocked` 在启动 `HixlBatchSend` Kernel 后立即调用
`aclrtSynchronizeStreamWithTimeout`，因此每个 connection group 都会在 Host 侧同步等待。

当一次 `Send` 包含多个 connection group 时，当前时序为：

```text
group A: launch -> synchronize
group B: launch -> synchronize
group C: launch -> synchronize
Send return
```

总耗时近似为各 group 耗时之和。但每个 connection 已拥有独立的 HCOMM channel、
HCOMM thread、ACL stream、mapped workspace 和 `sendMu`，因此不同 connection group 具备并行
执行条件。

## 2. 目标与非目标

### 2.1 目标

- 使同一次 `Send` 中的不同 connection group 并行执行 Kernel launch 和 stream synchronize。
- 保持 `TransProvider::Send` 对上层的同步语义不变。
- 保持当前 send buffer、flag buffer、workspace 和 connection 的生命周期不变。
- 由 `Send` 调用线程统一归并 group 级和 entry 级状态。

### 2.2 非目标

- 方案 1 不将 `TransProvider::Send` 改为真正的异步接口。
- 不改造 `TransportTaskExecutor` 的任务状态机。
- 不在本方案中引入 send ticket、ACL event 轮询或 provider completion callback。
- 不提升单个 connection 内的 Kernel 并发度；同一 connection 仍由 `sendMu` 串行化。

## 3. 方案 1：线程池并行 connection group，`Send` 保持同步

实现状态：已在个人验证分支实现。默认创建 4 个 send worker，可通过
`aicpu_send_worker_num` 调整；配置为 1 时保留原串行执行路径。

### 3.1 总体设计

`AICPUTransProvider::Impl` 持有一个专用、固定 worker 数的线程池。`Send` 完成所有入参校验和
connection 分组后，先将所有 group 任务提交到线程池，然后等待所有 future，最后按
`originalIndexes` 将结果写回输入顺序。

```text
Send caller
  |
  +-- submit group A -- worker 1: bind context -> lock A -> launch -> sync -> status
  +-- submit group B -- worker 2: bind context -> lock B -> launch -> sync -> status
  +-- submit group C -- worker 3: bind context -> lock C -> launch -> sync -> status
  |
  +-- wait all futures
  +-- merge all statuses
  +-- return
```

多 group 耗时预期从近似 `T(A) + T(B) + T(C)` 降为
`max(T(A), T(B), T(C)) + scheduling overhead`。

### 3.2 对外语义

`TransProvider::Send` 的签名和语义保持不变：

```cpp
std::vector<Status> Send(const std::vector<SendIoBatch>& ioBatches,
                         uint32_t kernelCount,
                         uint32_t quietCount);
```

`Send` 返回时仍保证：

- 所有已提交 group 的 `aclrtSynchronizeStreamWithTimeout` 已结束。
- HIXL `status_array` 已被读回 Host。
- 返回向量的顺序与输入 `ioBatches` 一致。
- 返回后运输层可以按现有流程轮询远端 `flagBuffer`。

因此上层 `TransportTaskExecutor`、`CompletionLoop`、cancel 和 shutdown 流程无需因方案 1
改变。

### 3.3 线程池任务

每个任务只处理一个 `ConnectionBatchGroup`，并返回独立结果：

```cpp
struct GroupSendResult {
    Status launchStatus;
    std::vector<std::uint32_t> hixlStatuses;
};
```

工作线程执行步骤：

1. 绑定 Provider 所在的 ACL device/context。
2. 获取该 connection 的 `sendMu`。
3. 调用现有 `LaunchBatchSendLocked`。
4. 完成 Kernel launch、stream synchronize 和 HIXL status 读取。
5. 返回 `GroupSendResult`。

建议伪代码：

```cpp
for (const auto& group : groups) {
    auto connection = group.connection;
    auto batches = group.batches;
    futures.push_back(sendPool.Submit(
        [this, connection, batches = std::move(batches)]() mutable {
            GroupSendResult result;

            const auto [deviceId, providerContext] = GetAclDeviceBinding();
            ScopedAclDeviceContext scope("SendWorker", deviceId, providerContext);
            if (!scope.status().ok()) {
                result.launchStatus = scope.status();
                return result;
            }

            std::lock_guard<std::mutex> lock(connection->sendMu);
            result.launchStatus = LaunchBatchSendLocked(
                *connection, batches, result.hixlStatuses);
            return result;
        }));
}

for (std::size_t i = 0; i < futures.size(); ++i) {
    auto groupResult = futures[i].get();
    MergeGroupResult(groups[i], groupResult, results);
}
```

必须先提交所有任务，再统一执行 `future.get()`。若在每次 `Submit` 后立即 `get()`，
执行仍然会退化为串行。

### 3.4 ACL Context

ACL current context 是线程相关状态。当前 `Send` 在调用线程中创建的
`ScopedAclDeviceContext` 不会自动传递给线程池 worker。

因此方案 1 必须：

- 将 ACL context 绑定移入每个 worker 任务。
- context 绑定失败时，将整个 group 标记为失败。
- 不依赖提交线程的 current context。

当前每个 connection 使用独立 stream，同一 connection 的 stream 操作由 `sendMu` 保证
顺序。这是不同 worker 共享 Provider context 时的基本保序条件。

### 3.5 锁和资源所有权

- `sendMu` 必须在 worker 内获取和释放，不能由提交线程提前持有。
- 每个 worker 在 `LaunchBatchSendLocked` 返回前一直持有 `sendMu`，与当前串行版本的
  connection-local 互斥语义一致。
- group task 必须持有 `std::shared_ptr<ConnectionRecord>`，以防止排队和执行期间
  connection record 被释放。
- task 必须拥有或安全引用 batch 描述符，直到 future 完成。
- 不同 worker 不直接写入共享 `results`，而是返回私有 `GroupSendResult`，由
  `Send` 调用线程统一归并。

### 3.6 Status 归并

归并规则保持现有语义：

- `launchStatus` 失败：该 group 的全部 `originalIndexes` 使用同一错误。
- `launchStatus` 成功：按 `hixlStatuses` 逐项映射。
- HIXL status 数量与 group 大小不匹配：该 group 全部返回 internal error。
- future 异常或线程池内部异常：该 group 全部返回 internal error。

某个 group 失败时不应提前从 `Send` 返回。已提交的其他 worker 可能仍在使用
connection、workspace 和 batch 数据，因此必须先等待全部 future 到达终态。

### 3.7 线程池建议

- 线程池归 `AICPUTransProvider::Impl` 所有，不在每次 `Send` 时创建。
- 使用 AICPU send 专用线程池，不与其他业务共享，避免长时间 stream synchronize
  占用通用 worker。
- worker 数量不应超过可并行 connection 数太多。当前提供
  `aicpu_send_worker_num` 配置，默认值为 4、上限为 64；应继续通过 1、2、4、8 等档位压测。
- 当前验证版使用无界任务队列。主调用路径由单个 `WorkerLoop` 调用 `Send`，单次排队量受
  connection group 数限制；如果后续开放多个调用方并发 `Send`，需要增加队列上限和明确的
  `RESOURCE_BUSY` 背压语义。
- `sendTimeoutMs` 可能很长，等待 stream 的任务会长时间占用 worker，因此不能使用
  过小的线程池。

### 3.8 析构顺序

`Impl` 析构时必须按以下顺序清理：

```text
stop accepting new send tasks
  -> drain and join send thread pool
  -> reset connection streams/workspaces
  -> unload HIXL binary
  -> destroy HCOMM endpoint
```

不能仅依赖线程池成员的默认析构顺序，因为 `Impl` 析构函数体当前会先释放
stream、workspace 和 HIXL binary。

### 3.9 局限性

- 这不是真正的非阻塞 Send；调用 `Send` 的 Host 线程仍然要等待所有 future。
- worker 仍会阻塞在 `aclrtSynchronizeStreamWithTimeout`。
- 只有一个 connection group 时没有并行收益。
- 多个调用方并发调用同一 Provider 时，多个 worker 可能阻塞在同一 `sendMu`，
  造成线程池头部阻塞。当前主调用路径是单 `WorkerLoop`，该风险较低。

## 4. 需同时修正的现有问题

`Send` 的参数校验当前使用 `valid` 做全局短路。只要一项无效，所有 group 都不会
发送，但其他合法项仍保留默认 `Status::OK()`。这会导致上层等待一个实际从未发送的
请求。

方案 1 采用全批拒绝语义：只要一项校验失败，原本合法但未提交的项返回 `CANCELED`，
校验失败项保留其具体错误。未采用“仅提交合法 group”的部分提交方式，避免同一次批量调用
出现部分已发送、部分未发送但调用方误判的情况。

这也修复了原逻辑中“全批实际未发送，但合法项仍返回 `OK`”的问题。

## 5. 验证计划

至少需要覆盖：

- 多 connection group 的 launch 时间窗口确实重叠。
- 单 connection group 的行为与改造前一致。
- group 成功、group 级失败、entry 级失败的原始下标映射正确。
- worker ACL context 绑定失败能够正确返回。
- 线程池任务抛异常时不会导致 `Send` 越界退出。
- 并发 `RegisterMemory`/`DeleteConnections` 与 Send 时不出现 workspace、stream 或 channel
  use-after-free。
- Provider 析构会先 drain 线程池，再释放 ACL/HCOMM 资源。
- 对比不同 worker 数下的总延迟、吞吐、CPU 占用和 ACL/HCOMM 错误率。

## 6. 后续方案讨论

### 6.1 方案 2：`LaunchBatchSendLocked` 整体异步化

待讨论项：

- launch 完成、HIXL send 完成和远端 KV 完成三个阶段的接口语义。
- send ticket/future/callback/poll 的选择。
- 每个 in-flight send 的独立 workspace 或 workspace pool。
- ACL event 的记录、查询、销毁和超时处理。
- send buffer、connection、stream 和 HCOMM thread 的所有权。
- cancel、timeout、connection recovery 和 shutdown drain。

### 6.2 方案 3：新增同步包装接口

候选命名：

```cpp
Status LaunchBatchSendLocked(...);      // 异步提交，返回 ticket
Status SyncLaunchBatchSendLocked(...);  // 同步兼容包装
```

或者使用语义更明确的命名：

```cpp
Status EnqueueBatchSendLocked(..., SendTicket& ticket);
Status WaitBatchSend(SendTicket& ticket, std::vector<std::uint32_t>& statuses);
Status SendBatchAndWaitLocked(..., std::vector<std::uint32_t>& statuses);
```

需要继续讨论同步包装的层级：它是否仅作为 AICPU Provider 内部兼容接口，还是需要
体现到 `TransProvider` 公共抽象中。
