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

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/types.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"

namespace gs {
namespace function {

template <typename... Fs>
static auto visit(const common::LogicalType& dataType, Fs... funcs) {
  auto func = common::overload(funcs...);
  switch (dataType.getLogicalTypeID()) {
  /* NOLINTBEGIN(bugprone-branch-clone)*/
  case common::LogicalTypeID::INT8:
    return func(int8_t());
  case common::LogicalTypeID::UINT8:
    return func(uint8_t());
  case common::LogicalTypeID::INT16:
    return func(int16_t());
  case common::LogicalTypeID::UINT16:
    return func(uint16_t());
  case common::LogicalTypeID::INT32:
    return func(int32_t());
  case common::LogicalTypeID::UINT32:
    return func(uint32_t());
  case common::LogicalTypeID::INT64:
    return func(int64_t());
  case common::LogicalTypeID::UINT64:
    return func(uint64_t());
  case common::LogicalTypeID::DOUBLE:
    return func(double());
  case common::LogicalTypeID::FLOAT:
    return func(float());
  /* NOLINTEND(bugprone-branch-clone)*/
  default:
    break;
  }
  // LCOV_EXCL_START
  THROW_RUNTIME_ERROR(common::stringFormat(
      "{} weight type is not supported for weighted shortest path.",
      dataType.toString()));
  // LCOV_EXCL_STOP
}

template <typename T>
static void checkWeight(T weight) {
  if (NEUG_UNLIKELY(weight < 0)) {
    THROW_RUNTIME_ERROR(
        common::stringFormat("Found negative weight {}. This is not supported "
                             "in weighted shortest path.",
                             weight));
  }
}

}  // namespace function
}  // namespace gs
