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
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "neug/storages/loader/loader_utils.h"

namespace neug {

namespace chunk_pipeline_detail {

inline int32_t hardware_worker_count() {
  auto workers = static_cast<int32_t>(std::thread::hardware_concurrency());
  return workers <= 0 ? 1 : workers;
}

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

/// Reads chunks from one non-thread-safe supplier and delivers them to a
/// bounded pool of consumers.  It preserves the first exception, closes the
/// queue on cancellation, and joins every thread before rethrowing.
template <typename Consume>
inline void consume_chunk_pipeline_impl(IDataChunkSupplier& supplier,
                                        int32_t consumer_count,
                                        size_t queue_capacity,
                                        Consume&& consume) {
  consumer_count = std::max<int32_t>(1, consumer_count);
  if (consumer_count == 1) {
    uint64_t next_row_ordinal = 0;
    while (auto chunk = supplier.GetNextChunk()) {
      const auto row_count = chunk->row_num();
      consume(0, SequencedDataChunk{std::move(chunk), next_row_ordinal});
      CHECK_LE(row_count,
               std::numeric_limits<uint64_t>::max() - next_row_ordinal);
      next_row_ordinal += static_cast<uint64_t>(row_count);
    }
    return;
  }

  chunk_pipeline_detail::BoundedQueue<SequencedDataChunk> queue(queue_capacity);
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
      uint64_t next_row_ordinal = 0;
      while (!cancelled.load(std::memory_order_acquire)) {
        auto chunk = supplier.GetNextChunk();
        if (!chunk) {
          break;
        }
        const auto row_count = chunk->row_num();
        if (!queue.Push({std::move(chunk), next_row_ordinal})) {
          break;
        }
        CHECK_LE(row_count,
                 std::numeric_limits<uint64_t>::max() - next_row_ordinal);
        next_row_ordinal += static_cast<uint64_t>(row_count);
      }
    } catch (...) { capture_error(std::current_exception()); }
    queue.Close();
  });

  std::vector<std::thread> consumers;
  consumers.reserve(static_cast<size_t>(consumer_count));
  for (int32_t i = 0; i < consumer_count; ++i) {
    consumers.emplace_back([&, i] {
      try {
        SequencedDataChunk chunk;
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

/// Concurrently pulls chunks from a supplier that owns its producer queue.
/// This avoids adding another producer thread and bounded queue between a
/// partitioned source and its consumers.
template <typename Consume>
inline void consume_concurrent_supplier_impl(IDataChunkSupplier& supplier,
                                             int32_t consumer_count,
                                             Consume&& consume) {
  CHECK(supplier.SupportsConcurrentGetNext());
  consumer_count = std::max<int32_t>(1, consumer_count);
  if (consumer_count == 1) {
    while (true) {
      auto chunk = supplier.GetNextChunkWithOrdinal();
      if (!chunk.chunk) {
        break;
      }
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
          auto chunk = supplier.GetNextChunkWithOrdinal();
          if (!chunk.chunk) {
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

}  // namespace chunk_pipeline_detail

/// Delivers chunks to indexed consumers using the cheapest path supported by
/// the supplier: direct concurrent pulls, serial iteration, or one producer
/// feeding a bounded consumer queue.
template <typename Consume>
inline void consume_supplier_indexed(IDataChunkSupplier& supplier,
                                     const ChunkSourceOptions& options,
                                     Consume&& consume) {
  const auto normalized = NormalizeChunkSourceOptions(options);
  auto consumer_count = normalized.consumer_count;
  if (supplier.SupportsConcurrentGetNext()) {
    chunk_pipeline_detail::consume_concurrent_supplier_impl(
        supplier, consumer_count, std::forward<Consume>(consume));
    return;
  }

  // A non-concurrent supplier needs an extra forwarding producer whenever
  // several consumers are used. Account for that thread at the execution
  // boundary as well; falling back to one inline consumer avoids the extra
  // thread when the remaining budget is too small.
  if (consumer_count > 1) {
    const auto max_consumers_with_forwarder =
        normalized.worker_budget - normalized.producer_count - 1;
    consumer_count = std::min(consumer_count, max_consumers_with_forwarder);
    if (consumer_count < 2) {
      consumer_count = 1;
    }
  }
  chunk_pipeline_detail::consume_chunk_pipeline_impl(
      supplier, consumer_count, normalized.queue_capacity,
      std::forward<Consume>(consume));
}

}  // namespace neug
