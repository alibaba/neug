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

#include <string>
#include <string_view>

#include "neug/common/types.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

namespace neug {

/**
 * @brief Compile-time map: C++ storage element type → short factory key
 * fragment.
 *
 * Used to build the `module_type` strings registered with ModuleFactory via
 * NEUG_REGISTER_TEMPLATE_MODULE.  Add a specialization here whenever a new
 * leaf storage type is introduced.  Types listed below correspond to the
 * registrations in:
 *   - src/utils/property/column.cc        (TypedColumn<T>)
 *   - src/storages/csr/mutable_csr.cc     (MutableCsr<T>, SingleMutableCsr<T>,
 *                                          EmptyCsr<T>)
 *   - src/storages/csr/immutable_csr.cc   (ImmutableCsr<T>,
 *                                          SingleImmutableCsr<T>)
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

namespace module_naming {

namespace detail {
inline std::string DataTypeShortName(DataTypeId t) {
  switch (t) {
  case DataTypeId::kEmpty:
    return StorageTypeName<EmptyType>::value;
  case DataTypeId::kBoolean:
    return StorageTypeName<bool>::value;
  case DataTypeId::kInt32:
    return StorageTypeName<int32_t>::value;
  case DataTypeId::kUInt32:
    return StorageTypeName<uint32_t>::value;
  case DataTypeId::kInt64:
    return StorageTypeName<int64_t>::value;
  case DataTypeId::kUInt64:
    return StorageTypeName<uint64_t>::value;
  case DataTypeId::kFloat:
    return StorageTypeName<float>::value;
  case DataTypeId::kDouble:
    return StorageTypeName<double>::value;
  case DataTypeId::kDate:
    return StorageTypeName<Date>::value;
  case DataTypeId::kTimestampMs:
    return StorageTypeName<DateTime>::value;
  case DataTypeId::kInterval:
    return StorageTypeName<Interval>::value;
  case DataTypeId::kVarchar:
    return StorageTypeName<std::string_view>::value;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("DataTypeShortName: unsupported DataTypeId " +
                                  std::to_string(static_cast<int>(t)));
  }
}

inline std::string CsrPrefix(EdgeStrategy strategy, bool is_mutable) {
  // Mirror src/storages/graph/edge_table.cc:create_csr_impl.
  switch (strategy) {
  case EdgeStrategy::kSingle:
    return is_mutable ? "single_mutable_csr" : "single_immutable_csr";
  case EdgeStrategy::kMultiple:
    return is_mutable ? "mutable_csr" : "immutable_csr";
  case EdgeStrategy::kNone:
    // EmptyCsr is the only choice and ignores the is_mutable flag — see
    // create_csr_impl.
    return "empty_csr";
  }
  THROW_NOT_SUPPORTED_EXCEPTION("CsrPrefix: invalid EdgeStrategy " +
                                std::to_string(static_cast<int>(strategy)));
}
}  // namespace detail

/// Canonical factory key for a CSR Module given edge-data type, strategy, and
/// mutability.  Mirrors `src/storages/graph/edge_table.cc:create_csr_impl`.
inline std::string CsrTypeName(DataTypeId edge_data_type, EdgeStrategy strategy,
                               bool is_mutable) {
  return detail::CsrPrefix(strategy, is_mutable) + "<" +
         detail::DataTypeShortName(edge_data_type) + ">";
}

}  // namespace module_naming

}  // namespace neug
