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

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/types/value/value.h"
namespace neug {
namespace binder {

struct BoundQueryScanSourceInfo {
  common::case_insensitive_map_t<common::Value> options;

  explicit BoundQueryScanSourceInfo(
      common::case_insensitive_map_t<common::Value> options)
      : options{std::move(options)} {}
};

}  // namespace binder
}  // namespace neug
