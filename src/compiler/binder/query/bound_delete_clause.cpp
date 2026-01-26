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

#include "neug/compiler/binder/query/updating_clause/bound_delete_clause.h"

using namespace neug::common;

namespace neug {
namespace binder {

bool BoundDeleteClause::hasInfo(
    const std::function<bool(const BoundDeleteInfo&)>& check) const {
  for (auto& info : infos) {
    if (check(info)) {
      return true;
    }
  }
  return false;
}

std::vector<BoundDeleteInfo> BoundDeleteClause::getInfos(
    const std::function<bool(const BoundDeleteInfo&)>& check) const {
  std::vector<BoundDeleteInfo> result;
  for (auto& info : infos) {
    if (check(info)) {
      result.push_back(info.copy());
    }
  }
  return result;
}

}  // namespace binder
}  // namespace neug
