/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string_view>

#include "neug/utils/property/types.h"

namespace neug {

/**
 * @brief Type trait that maps a C++ storage element type to a short name
 * string used for ModuleFactory registration keys.
 *
 * Specializations are provided for all types that appear as template
 * parameters in TypedColumn<T>, MutableCsr<T>, ImmutableCsr<T>, etc.
 */
template <typename T>
struct StorageTypeName {
  // Unknown / unregistered type — left intentionally empty so that SFINAE or
  // a static_assert in the caller can catch it.
  static constexpr const char* value = "unknown";
};

#define DEFINE_STORAGE_TYPE_NAME(CppType, Name) \
  template <>                                   \
  struct StorageTypeName<CppType> {             \
    static constexpr const char* value = Name;  \
  }

DEFINE_STORAGE_TYPE_NAME(EmptyType, "empty");
DEFINE_STORAGE_TYPE_NAME(bool, "bool");
DEFINE_STORAGE_TYPE_NAME(uint8_t, "uint8");
DEFINE_STORAGE_TYPE_NAME(uint16_t, "uint16");
DEFINE_STORAGE_TYPE_NAME(int32_t, "int32");
DEFINE_STORAGE_TYPE_NAME(uint32_t, "uint32");
DEFINE_STORAGE_TYPE_NAME(int64_t, "int64");
DEFINE_STORAGE_TYPE_NAME(uint64_t, "uint64");
DEFINE_STORAGE_TYPE_NAME(float, "float");
DEFINE_STORAGE_TYPE_NAME(double, "double");
DEFINE_STORAGE_TYPE_NAME(Date, "date");
DEFINE_STORAGE_TYPE_NAME(DateTime, "datetime");
DEFINE_STORAGE_TYPE_NAME(Interval, "interval");
DEFINE_STORAGE_TYPE_NAME(std::string_view, "string");

#undef DEFINE_STORAGE_TYPE_NAME

template <typename T>
inline std::string type_name_string() {
  return StorageTypeName<T>::value;
}

}  // namespace neug
