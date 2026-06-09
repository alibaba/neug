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

#include <type_traits>

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/types/timestamp_t.h"
#include "neug/compiler/common/types/types.h"

#include <glog/logging.h>

namespace neug {
namespace common {

class ValueVector;

template <class... Funcs>
struct overload : Funcs... {
  explicit overload(Funcs... funcs) : Funcs(funcs)... {}
  using Funcs::operator()...;
};

class TypeUtils {
 public:
  template <typename Func, typename... Types, size_t... indices>
  static void paramPackForEachHelper(const Func& func,
                                     std::index_sequence<indices...>,
                                     Types&&... values) {
    ((func(indices, values)), ...);
  }

  template <typename Func, typename... Types>
  static void paramPackForEach(const Func& func, Types&&... values) {
    paramPackForEachHelper(func, std::index_sequence_for<Types...>(),
                           std::forward<Types>(values)...);
  }

  static std::string entryToString(const DataType& dataType,
                                   const uint8_t* value, ValueVector* vector);

  template <typename T>
  static inline std::string toString(const T& val,
                                     void* /*valueVector*/ = nullptr) {
    if constexpr (std::is_same_v<T, std::string>) {
      return val;
    } else if constexpr (std::is_same_v<T, neug_string_t>) {
      return val.getAsString();
    } else {
      static_assert(
          std::is_same<T, int64_t>::value || std::is_same<T, int32_t>::value ||
          std::is_same<T, int16_t>::value || std::is_same<T, int8_t>::value ||
          std::is_same<T, uint64_t>::value ||
          std::is_same<T, uint32_t>::value ||
          std::is_same<T, uint16_t>::value || std::is_same<T, uint8_t>::value ||
          std::is_same<T, double>::value || std::is_same<T, float>::value);
      return std::to_string(val);
    }
  }
  static std::string nodeToString(const struct_entry_t& val,
                                  ValueVector* vector);
  static std::string relToString(const struct_entry_t& val,
                                 ValueVector* vector);

  static inline void encodeOverflowPtr(uint64_t& overflowPtr,
                                       page_idx_t pageIdx,
                                       uint32_t pageOffset) {
    memcpy(&overflowPtr, &pageIdx, 4);
    memcpy(((uint8_t*) &overflowPtr) + 4, &pageOffset, 4);
  }
  static inline void decodeOverflowPtr(uint64_t overflowPtr,
                                       page_idx_t& pageIdx,
                                       uint32_t& pageOffset) {
    pageIdx = 0;
    memcpy(&pageIdx, &overflowPtr, 4);
    memcpy(&pageOffset, ((uint8_t*) &overflowPtr) + 4, 4);
  }

  template <typename T>
  static inline constexpr common::PhysicalTypeID getPhysicalTypeIDForType() {
    if constexpr (std::is_same_v<T, int64_t>) {
      return common::PhysicalTypeID::INT64;
    } else if constexpr (std::is_same_v<T, int32_t>) {
      return common::PhysicalTypeID::INT32;
    } else if constexpr (std::is_same_v<T, int16_t>) {
      return common::PhysicalTypeID::INT16;
    } else if constexpr (std::is_same_v<T, int8_t>) {
      return common::PhysicalTypeID::INT8;
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      return common::PhysicalTypeID::UINT64;
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      return common::PhysicalTypeID::UINT32;
    } else if constexpr (std::is_same_v<T, uint16_t>) {
      return common::PhysicalTypeID::UINT16;
    } else if constexpr (std::is_same_v<T, uint8_t>) {
      return common::PhysicalTypeID::UINT8;
    } else if constexpr (std::is_same_v<T, float>) {
      return common::PhysicalTypeID::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
      return common::PhysicalTypeID::DOUBLE;
    } else if constexpr (std::is_same_v<T, int128_t>) {
      return common::PhysicalTypeID::INT128;
    } else if constexpr (std::is_same_v<T, interval_t>) {
      return common::PhysicalTypeID::INTERVAL;
    } else if constexpr (std::same_as<T, neug_string_t> ||
                         std::same_as<T, std::string> ||
                         std::same_as<T, std::string_view>) {
      return common::PhysicalTypeID::STRING;
    } else {
      NEUG_UNREACHABLE;
    }
  }

  /*
   * TypeUtils::visit can be used to call generic code on all or some Logical
   * and Physical type variants with access to type information.
   *
   * E.g.
   *
   *  std::string result;
   *  visit(dataType, [&]<typename T>(T) {
   *      if constexpr(std::is_same_v<T, neug_string_t>()) {
   *          result = vector->getValue<neug_string_t>(0).getAsString();
   *      } else if (std::integral<T>) {
   *          result = std::to_string(vector->getValue<T>(0));
   *      } else {
   *          NEUG_UNREACHABLE;
   *      }
   *  });
   *
   * or
   *  std::string result;
   *  visit(dataType,
   *      [&](neug_string_t) {
   *          result = vector->getValue<neug_string_t>(0);
   *      },
   *      [&]<std::integral T>(T) {
   *          result = std::to_string(vector->getValue<T>(0));
   *      },
   *      [](auto) { NEUG_UNREACHABLE; }
   *  );
   *
   * Note that when multiple functions are provided, at least one function must
   * match all data types.
   *
   * Also note that implicit conversions may occur with the multi-function
   * variant if you don't include a generic auto function to cover types which
   * aren't explicitly included. See
   * https://en.cppreference.com/w/cpp/utility/variant/visit
   */
  template <typename... Fs>
  static inline auto visit(const DataType& dataType, Fs... funcs) {
    // Note: arguments are used only for type deduction and have no meaningful
    // value. They should be optimized out by the compiler
    auto func = overload(funcs...);
    switch (dataType.id()) {
    /* NOLINTBEGIN(bugprone-branch-clone)*/
    case DataTypeId::kInt8:
      return func(int8_t());
    case DataTypeId::kUInt8:
      return func(uint8_t());
    case DataTypeId::kInt16:
      return func(int16_t());
    case DataTypeId::kUInt16:
      return func(uint16_t());
    case DataTypeId::kInt32:
      return func(int32_t());
    case DataTypeId::kUInt32:
      return func(uint32_t());
    case DataTypeId::kInt64:
      return func(int64_t());
    case DataTypeId::kUInt64:
      return func(uint64_t());
    case DataTypeId::kBoolean:
      return func(bool());
    case DataTypeId::kDouble:
      return func(double());
    case DataTypeId::kFloat:
      return func(float());
    case DataTypeId::kInterval:
      return func(interval_t());
    case DataTypeId::kInternalId:
      return func(internalID_t());
    case DataTypeId::kVarchar:
      return func(neug_string_t());
    case DataTypeId::kDate:
      return func(date_t());
    case DataTypeId::kTimestampMs:
      return func(timestamp_ms_t());
    case DataTypeId::kArray:
    case DataTypeId::kList:
      return func(list_entry_t());
    case DataTypeId::kMap:
      return func(map_entry_t());
    case DataTypeId::kVertex:
    case DataTypeId::kEdge:
    case DataTypeId::kPath:
    case DataTypeId::kStruct:
      return func(struct_entry_t());
    /* NOLINTEND(bugprone-branch-clone)*/
    default:
      // Unsupported type
      NEUG_UNREACHABLE;
    }
  }

  template <typename... Fs>
  static inline auto visit(PhysicalTypeID dataType, Fs&&... funcs) {
    // Note: arguments are used only for type deduction and have no meaningful
    // value. They should be optimized out by the compiler
    auto func = overload(funcs...);
    switch (dataType) {
    /* NOLINTBEGIN(bugprone-branch-clone)*/
    case PhysicalTypeID::INT8:
      return func(int8_t());
    case PhysicalTypeID::UINT8:
      return func(uint8_t());
    case PhysicalTypeID::INT16:
      return func(int16_t());
    case PhysicalTypeID::UINT16:
      return func(uint16_t());
    case PhysicalTypeID::INT32:
      return func(int32_t());
    case PhysicalTypeID::UINT32:
      return func(uint32_t());
    case PhysicalTypeID::INT64:
      return func(int64_t());
    case PhysicalTypeID::UINT64:
      return func(uint64_t());
    case PhysicalTypeID::BOOL:
      return func(bool());
    case PhysicalTypeID::INT128:
      return func(int128_t());
    case PhysicalTypeID::DOUBLE:
      return func(double());
    case PhysicalTypeID::FLOAT:
      return func(float());
    case PhysicalTypeID::INTERVAL:
      return func(interval_t());
    case PhysicalTypeID::INTERNAL_ID:
      return func(internalID_t());
    case PhysicalTypeID::STRING:
      return func(neug_string_t());
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
      return func(list_entry_t());
    case PhysicalTypeID::STRUCT:
      return func(struct_entry_t());
    /* NOLINTEND(bugprone-branch-clone)*/
    case PhysicalTypeID::ANY:
    case PhysicalTypeID::ALP_EXCEPTION_DOUBLE:
    case PhysicalTypeID::ALP_EXCEPTION_FLOAT:
      // Unsupported type
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported physical type " +
                                    std::to_string(static_cast<int>(dataType)));
      NEUG_UNREACHABLE;
      // Needed for return type deduction to work
      return func(uint8_t());
    default:
      NEUG_UNREACHABLE;
    }
  }
};

// Forward declaration of template specializations.
template <>
std::string TypeUtils::toString(const int128_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const bool& val, void* valueVector);
template <>
std::string TypeUtils::toString(const internalID_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const date_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const timestamp_ms_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const timestamp_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const interval_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const neug_string_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const list_entry_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const map_entry_t& val, void* valueVector);
template <>
std::string TypeUtils::toString(const struct_entry_t& val, void* valueVector);

}  // namespace common
}  // namespace neug
