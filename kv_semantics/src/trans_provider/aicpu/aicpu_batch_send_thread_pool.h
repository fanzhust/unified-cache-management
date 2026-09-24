/**
 * MIT License
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#pragma once

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace kv::aicpu_detail {

class BatchSendThreadPool final {
public:
    explicit BatchSendThreadPool(std::size_t workerCount)
    {
        workerCount = std::max<std::size_t>(workerCount, 1U);
        workers_.reserve(workerCount);
        try {
            for (std::size_t i = 0; i < workerCount; ++i) {
                workers_.emplace_back([this] { WorkerLoop(); });
            }
        } catch (...) {
            Shutdown();
            throw;
        }
    }

    ~BatchSendThreadPool() { Shutdown(); }

    BatchSendThreadPool(const BatchSendThreadPool&) = delete;
    BatchSendThreadPool& operator=(const BatchSendThreadPool&) = delete;

    template <typename Handler>
    auto Submit(Handler&& handler)
        -> std::future<std::invoke_result_t<std::decay_t<Handler>&>>
    {
        using Result = std::invoke_result_t<std::decay_t<Handler>&>;
        auto task =
            std::make_shared<std::packaged_task<Result()>>(std::forward<Handler>(handler));
        auto future = task->get_future();
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopping_) {
                throw std::runtime_error("AICPU batch send thread pool is stopping");
            }
            tasks_.emplace_back([task] { (*task)(); });
        }
        ready_.notify_one();
        return future;
    }

    void Shutdown()
    {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stopping_ = true;
        }
        ready_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) { worker.join(); }
        }
        workers_.clear();
    }

private:
    void WorkerLoop()
    {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mu_);
                ready_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
                if (stopping_ && tasks_.empty()) { return; }
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            task();
        }
    }

    std::mutex mu_;
    std::condition_variable ready_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    bool stopping_{false};
};

}  // namespace kv::aicpu_detail
