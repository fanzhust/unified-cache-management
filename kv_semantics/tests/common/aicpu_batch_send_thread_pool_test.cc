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
 * */

#include "aicpu_batch_send_thread_pool.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <gtest/gtest.h>
#include <stdexcept>
#include <thread>

namespace kv::test {
namespace {

using namespace std::chrono_literals;

TEST(AicpuBatchSendThreadPoolTest, ReturnsTaskResults)
{
    aicpu_detail::BatchSendThreadPool pool(2);
    auto first = pool.Submit([] { return 11; });
    auto second = pool.Submit([] { return 31; });

    EXPECT_EQ(first.get() + second.get(), 42);
}

TEST(AicpuBatchSendThreadPoolTest, ExecutesIndependentTasksConcurrently)
{
    aicpu_detail::BatchSendThreadPool pool(2);
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::atomic<std::uint32_t> entered{0};

    auto task = [&entered, gate] {
        entered.fetch_add(1, std::memory_order_release);
        gate.wait();
    };
    auto first = pool.Submit(task);
    auto second = pool.Submit(task);

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (entered.load(std::memory_order_acquire) != 2U &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const auto concurrent = entered.load(std::memory_order_acquire) == 2U;
    release.set_value();
    first.get();
    second.get();

    EXPECT_TRUE(concurrent);
}

TEST(AicpuBatchSendThreadPoolTest, DrainsQueuedTasksDuringShutdown)
{
    aicpu_detail::BatchSendThreadPool pool(1);
    std::atomic<std::uint32_t> completed{0};
    auto first = pool.Submit([&completed] { completed.fetch_add(1, std::memory_order_relaxed); });
    auto second = pool.Submit([&completed] { completed.fetch_add(1, std::memory_order_relaxed); });

    pool.Shutdown();
    first.get();
    second.get();
    EXPECT_EQ(completed.load(std::memory_order_relaxed), 2U);
}

TEST(AicpuBatchSendThreadPoolTest, RejectsSubmissionAfterShutdown)
{
    aicpu_detail::BatchSendThreadPool pool(1);
    pool.Shutdown();

    EXPECT_THROW((void)pool.Submit([] {}), std::runtime_error);
}

}  // namespace
}  // namespace kv::test
