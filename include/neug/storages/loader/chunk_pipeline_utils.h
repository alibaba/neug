/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "neug/storages/loader/loader_utils.h"

namespace neug {

struct ChunkPipelineOptions {
  int32_t consumer_count = 1;
  size_t queue_capacity = 2;
};

struct ChunkPipelineAllocation {
  bool parallel_enabled = false;
  int32_t producer_count = 1;
  int32_t consumer_count = 1;
  size_t queue_capacity = 2;
};

inline int32_t hardware_worker_count() {
  auto workers = static_cast<int32_t>(std::thread::hardware_concurrency());
  return workers <= 0 ? 1 : workers;
}

inline ChunkPipelineAllocation resolve_chunk_pipeline_allocation(
    int64_t source_bytes, bool parallel_enabled, bool preserve_order,
    int32_t hardware_workers = 0) {
  constexpr int64_t kMinParallelBytes = 256LL * 1024 * 1024;
  constexpr int64_t kMinPartitionBytes = 64LL * 1024 * 1024;
  constexpr size_t kMaxQueuedChunks = 64;

  ChunkPipelineAllocation result;
  const auto workers =
      hardware_workers > 0 ? hardware_workers : hardware_worker_count();
  if (!parallel_enabled || preserve_order || workers <= 1 ||
      source_bytes < kMinParallelBytes) {
    return result;
  }

  // Compute ceil(source_bytes / kMinPartitionBytes) without overflowing when
  // source_bytes is close to INT64_MAX.
  const auto useful_partitions = std::max<int64_t>(
      1, source_bytes / kMinPartitionBytes +
             (source_bytes % kMinPartitionBytes == 0 ? 0 : 1));
  const auto balanced_producers = (workers + 1) / 2;
  result.producer_count = static_cast<int32_t>(std::min<int64_t>(
      balanced_producers, std::min<int64_t>(useful_partitions, workers - 1)));
  result.producer_count = std::max<int32_t>(1, result.producer_count);
  result.consumer_count = std::max<int32_t>(1, workers - result.producer_count);
  result.parallel_enabled = true;
  result.queue_capacity = std::clamp<size_t>(
      static_cast<size_t>(result.producer_count) * 2, 2, kMaxQueuedChunks);
  return result;
}

namespace chunk_pipeline_detail {

template <typename T>
class BoundedQueue {
 public:
  explicit BoundedQueue(size_t capacity)
      : capacity_(std::max<size_t>(1, capacity)) {}

  bool Push(T value) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [&] { return closed_ || values_.size() < capacity_; });
    if (closed_) {
      return false;
    }
    values_.push_back(std::move(value));
    not_empty_.notify_one();
    return true;
  }

  bool Pop(T& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [&] { return closed_ || !values_.empty(); });
    if (values_.empty()) {
      return false;
    }
    value = std::move(values_.front());
    values_.pop_front();
    not_full_.notify_one();
    return true;
  }

  void Close() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
  }

 private:
  size_t capacity_;
  std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<T> values_;
  bool closed_ = false;
};

}  // namespace chunk_pipeline_detail

/// Reads chunks from one non-thread-safe supplier and delivers them to a
/// bounded pool of consumers.  It preserves the first exception, closes the
/// queue on cancellation, and joins every thread before rethrowing.
template <typename Consume>
inline void consume_chunk_pipeline_impl(IDataChunkSupplier& supplier,
                                        ChunkPipelineOptions options,
                                        Consume&& consume) {
  const auto consumer_count = std::max<int32_t>(1, options.consumer_count);
  if (consumer_count == 1) {
    while (auto chunk = supplier.GetNextChunk()) {
      consume(0, chunk);
    }
    return;
  }

  chunk_pipeline_detail::BoundedQueue<std::shared_ptr<DataChunk>> queue(
      options.queue_capacity);
  std::atomic<bool> cancelled{false};
  std::mutex error_mutex;
  std::exception_ptr first_error;
  auto capture_error = [&](std::exception_ptr error) {
    bool expected = false;
    if (cancelled.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
      {
        std::lock_guard<std::mutex> lock(error_mutex);
        first_error = std::move(error);
      }
      queue.Close();
      // A producer may be blocked inside GetNextChunk() when a consumer fails.
      // Closing the local queue only wakes this pipeline's threads; cancelling
      // the supplier is what gives it a chance to stop background work and
      // unblock the producer before join().
      supplier.Cancel();
    }
  };

  std::thread producer([&] {
    try {
      while (!cancelled.load(std::memory_order_acquire)) {
        auto chunk = supplier.GetNextChunk();
        if (!chunk || !queue.Push(std::move(chunk))) {
          break;
        }
      }
    } catch (...) { capture_error(std::current_exception()); }
    queue.Close();
  });

  std::vector<std::thread> consumers;
  consumers.reserve(static_cast<size_t>(consumer_count));
  for (int32_t i = 0; i < consumer_count; ++i) {
    consumers.emplace_back([&, i] {
      try {
        std::shared_ptr<DataChunk> chunk;
        while (!cancelled.load(std::memory_order_acquire) && queue.Pop(chunk)) {
          consume(i, chunk);
        }
      } catch (...) { capture_error(std::current_exception()); }
    });
  }

  producer.join();
  for (auto& consumer : consumers) {
    consumer.join();
  }
  if (first_error) {
    std::rethrow_exception(first_error);
  }
}

inline void consume_chunk_pipeline(
    IDataChunkSupplier& supplier, ChunkPipelineOptions options,
    const std::function<void(const std::shared_ptr<DataChunk>&)>& consume) {
  consume_chunk_pipeline_impl(
      supplier, options,
      [&](int32_t /*consumer*/, const std::shared_ptr<DataChunk>& chunk) {
        consume(chunk);
      });
}

inline void consume_chunk_pipeline_indexed(
    IDataChunkSupplier& supplier, ChunkPipelineOptions options,
    const std::function<void(int32_t, const std::shared_ptr<DataChunk>&)>&
        consume) {
  consume_chunk_pipeline_impl(supplier, options, consume);
}

/// Concurrently pulls chunks from a supplier that owns its producer queue.
/// This avoids adding another producer thread and bounded queue between a
/// partitioned source and its consumers.
template <typename Consume>
inline void consume_concurrent_supplier_indexed(IDataChunkSupplier& supplier,
                                                int32_t consumer_count,
                                                Consume&& consume) {
  CHECK(supplier.SupportsConcurrentGetNext());
  consumer_count = std::max<int32_t>(1, consumer_count);
  if (consumer_count == 1) {
    while (auto chunk = supplier.GetNextChunk()) {
      consume(0, chunk);
    }
    return;
  }

  std::atomic<bool> cancelled{false};
  std::mutex error_mutex;
  std::exception_ptr first_error;
  auto capture_error = [&](std::exception_ptr error) {
    bool expected = false;
    if (cancelled.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
      {
        std::lock_guard<std::mutex> lock(error_mutex);
        first_error = std::move(error);
      }
      supplier.Cancel();
    }
  };

  std::vector<std::thread> consumers;
  consumers.reserve(static_cast<size_t>(consumer_count));
  for (int32_t i = 0; i < consumer_count; ++i) {
    consumers.emplace_back([&, i] {
      try {
        while (!cancelled.load(std::memory_order_acquire)) {
          auto chunk = supplier.GetNextChunk();
          if (!chunk) {
            break;
          }
          consume(i, chunk);
        }
      } catch (...) { capture_error(std::current_exception()); }
    });
  }
  for (auto& consumer : consumers) {
    consumer.join();
  }
  if (first_error) {
    std::rethrow_exception(first_error);
  }
}

}  // namespace neug
