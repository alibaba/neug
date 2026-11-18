/** Copyright 2020 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <exception>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "neug/utils/result.h"

namespace neug {

struct ParallelOptions {
  size_t max_workers = 1;  // Includes the calling thread; must be positive.
  size_t chunk_size = 64;
};

// Calls func(index, worker_id) over [begin, end). Worker IDs are exclusive and
// smaller than max_workers. The callback returns void or Status. All workers
// are joined before returning a failure or rethrowing a callback exception.
// Failure cancels remaining work, but does not roll back callback side effects.
template <typename FUNC>
Status parallel_for(size_t begin, size_t end, const ParallelOptions& options,
                    const FUNC& func) {
  using Return = std::invoke_result_t<const FUNC&, size_t, size_t>;
  static_assert(std::is_void_v<Return> || std::is_same_v<Return, Status>);
  if (options.max_workers == 0 || options.chunk_size == 0 || begin > end) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Invalid parallel_for range or options");
  }
  if (begin == end)
    return Status::OK();
  const auto chunks = 1 + (end - begin - 1) / options.chunk_size;
  const auto workers = std::min(options.max_workers, chunks);
  std::atomic<size_t> next{begin};
  std::atomic<bool> cancelled{false};
  std::mutex error_mutex;
  Status status = Status::OK();
  std::exception_ptr exception;
  auto record_exception = [&] {
    std::lock_guard<std::mutex> lock(error_mutex);
    if (!cancelled.exchange(true))
      exception = std::current_exception();
  };
  auto run = [&](size_t worker_id) {
    try {
      while (!cancelled.load()) {
        size_t first = next.load();
        size_t last;
        do {
          if (first == end)
            return;
          last = first + std::min(options.chunk_size, end - first);
        } while (!next.compare_exchange_weak(first, last));
        for (size_t i = first; i < last && !cancelled.load(); ++i) {
          if constexpr (std::is_void_v<Return>) {
            func(i, worker_id);
          } else {
            auto result = func(i, worker_id);
            if (!result.ok()) {
              std::lock_guard<std::mutex> lock(error_mutex);
              if (!cancelled.exchange(true))
                status = std::move(result);
              return;
            }
          }
        }
      }
    } catch (...) { record_exception(); }
  };
  std::vector<std::thread> threads;
  threads.reserve(workers - 1);
  try {
    for (size_t i = 1; i < workers; ++i)
      threads.emplace_back(run, i);
    run(0);
  } catch (...) { record_exception(); }
  for (auto& thread : threads)
    thread.join();
  if (exception)
    std::rethrow_exception(exception);
  return status;
}

}  // namespace neug
