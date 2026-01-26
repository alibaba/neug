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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/serializer/writer.h"

namespace neug {
namespace common {

class NEUG_API Serializer {
 public:
  explicit Serializer(std::shared_ptr<Writer> writer)
      : writer(std::move(writer)) {}

  template <typename T>
      requires std::is_trivially_destructible<T>::value ||
      std::is_same<std::string, T>::value void serializeValue(const T& value) {
    writer->write((uint8_t*) &value, sizeof(T));
  }

  // Alias for serializeValue
  template <typename T>
  void write(const T& value) {
    serializeValue(value);
  }

  void writeDebuggingInfo(const std::string& value);

  void write(const uint8_t* value, uint64_t len) { writer->write(value, len); }

  template <typename T>
  void serializeOptionalValue(const std::unique_ptr<T>& value) {
    serializeValue(value == nullptr);
    if (value != nullptr) {
      value->serialize(*this);
    }
  }

  template <typename T1, typename T2>
  void serializeMap(const std::map<T1, T2>& values) {
    uint64_t mapSize = values.size();
    serializeValue(mapSize);
    for (auto& value : values) {
      serializeValue(value.first);
      value.second.serialize(*this);
    }
  }

  template <typename T1, typename T2>
  void serializeUnorderedMap(const std::unordered_map<T1, T2>& values) {
    uint64_t mapSize = values.size();
    serializeValue(mapSize);
    for (auto& value : values) {
      serializeValue(value.first);
      serializeValue<T2>(value.second);
    }
  }

  template <typename T1, typename T2>
  void serializeUnorderedMapOfPtrs(
      const std::unordered_map<T1, std::unique_ptr<T2>>& values) {
    uint64_t mapSize = values.size();
    serializeValue(mapSize);
    for (auto& value : values) {
      serializeValue(value.first);
      value.second->serialize(*this);
    }
  }

  template <typename T>
  void serializeVector(const std::vector<T>& values) {
    uint64_t vectorSize = values.size();
    serializeValue<uint64_t>(vectorSize);
    for (auto& value : values) {
      if constexpr (requires(Serializer & ser) { value.serialize(ser); }) {
        value.serialize(*this);
      } else {
        serializeValue<T>(value);
      }
    }
  }

  template <typename T, uint64_t ARRAY_SIZE>
  void serializeArray(const std::array<T, ARRAY_SIZE>& values) {
    for (auto& value : values) {
      if constexpr (requires(Serializer & ser) { value.serialize(ser); }) {
        value.serialize(*this);
      } else {
        serializeValue<T>(value);
      }
    }
  }

  template <typename T>
  void serializeVectorOfPtrs(const std::vector<std::unique_ptr<T>>& values) {
    uint64_t vectorSize = values.size();
    serializeValue<uint64_t>(vectorSize);
    for (auto& value : values) {
      value->serialize(*this);
    }
  }

  template <typename T>
  void serializeUnorderedSet(const std::unordered_set<T>& values) {
    uint64_t setSize = values.size();
    serializeValue(setSize);
    for (const auto& value : values) {
      serializeValue(value);
    }
  }

  void serializeCaseInsensitiveSet(const common::case_insensitve_set_t& values);

 private:
  std::shared_ptr<Writer> writer;
};

template <>
void Serializer::serializeValue(const std::string& value);

}  // namespace common
}  // namespace neug
