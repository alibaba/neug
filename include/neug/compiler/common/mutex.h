/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <mutex>
#include <optional>

#include "neug/compiler/common/copy_constructors.h"

namespace neug {
namespace common {

template <typename T>
class MutexGuard {
  template <typename T2>
  friend class Mutex;
  MutexGuard(T& data, std::unique_lock<std::mutex> lck)
      : data(&data), lck(std::move(lck)) {}

 public:
  DELETE_COPY_DEFAULT_MOVE(MutexGuard);

  T* operator->() & { return data; }
  T& operator*() & { return *data; }
  T* get() & { return data; }

  // Must not call these operators on a temporary MutexGuard!
  // Guards _must_ be held while accessing the inner data.
  T* operator->() && = delete;
  T& operator*() && = delete;
  T* get() && = delete;

 private:
  T* data;
  std::unique_lock<std::mutex> lck;
};

template <typename T>
class Mutex {
 public:
  Mutex() : data() {}
  explicit Mutex(T data) : data(std::move(data)) {}
  DELETE_COPY_AND_MOVE(Mutex);

  MutexGuard<T> lock() {
    std::unique_lock lck{mtx};
    return MutexGuard(data, std::move(lck));
  }

  std::optional<MutexGuard<T>> try_lock() {
    if (!mtx.try_lock()) {
      return std::nullopt;
    }
    std::unique_lock lck{mtx, std::adopt_lock};
    return MutexGuard(data, std::move(lck));
  }

 private:
  T data;
  std::mutex mtx;
};

}  // namespace common
}  // namespace neug
