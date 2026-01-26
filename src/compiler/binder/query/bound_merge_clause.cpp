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

#include "neug/compiler/binder/query/updating_clause/bound_merge_clause.h"

using namespace neug::common;

namespace neug {
namespace binder {

bool BoundMergeClause::hasInsertInfo(
    const std::function<bool(const BoundInsertInfo&)>& check) const {
  for (auto& info : insertInfos) {
    if (check(info)) {
      return true;
    }
  }
  return false;
}

std::vector<const BoundInsertInfo*> BoundMergeClause::getInsertInfos(
    const std::function<bool(const BoundInsertInfo&)>& check) const {
  std::vector<const BoundInsertInfo*> result;
  for (auto& info : insertInfos) {
    if (check(info)) {
      result.push_back(&info);
    }
  }
  return result;
}

bool BoundMergeClause::hasOnMatchSetInfo(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
  for (auto& info : onMatchSetPropertyInfos) {
    if (check(info)) {
      return true;
    }
  }
  return false;
}

std::vector<BoundSetPropertyInfo> BoundMergeClause::getOnMatchSetInfos(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
  std::vector<BoundSetPropertyInfo> result;
  for (auto& info : onMatchSetPropertyInfos) {
    if (check(info)) {
      result.push_back(info.copy());
    }
  }
  return result;
}

bool BoundMergeClause::hasOnCreateSetInfo(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
  for (auto& info : onCreateSetPropertyInfos) {
    if (check(info)) {
      return true;
    }
  }
  return false;
}

std::vector<BoundSetPropertyInfo> BoundMergeClause::getOnCreateSetInfos(
    const std::function<bool(const BoundSetPropertyInfo&)>& check) const {
  std::vector<BoundSetPropertyInfo> result;
  for (auto& info : onCreateSetPropertyInfos) {
    if (check(info)) {
      result.push_back(info.copy());
    }
  }
  return result;
}

}  // namespace binder
}  // namespace neug
