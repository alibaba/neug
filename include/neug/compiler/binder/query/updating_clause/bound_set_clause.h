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

#include "bound_set_info.h"
#include "bound_updating_clause.h"

namespace neug {
namespace binder {

class BoundSetClause final : public BoundUpdatingClause {
 public:
  BoundSetClause() : BoundUpdatingClause{common::ClauseType::SET} {}

  void addInfo(BoundSetPropertyInfo info) { infos.push_back(std::move(info)); }
  const std::vector<BoundSetPropertyInfo>& getInfos() const { return infos; }

  bool hasNodeInfo() const {
    return hasInfo([](const BoundSetPropertyInfo& info) {
      return info.tableType == common::TableType::NODE;
    });
  }
  std::vector<BoundSetPropertyInfo> getNodeInfos() const {
    return getInfos([](const BoundSetPropertyInfo& info) {
      return info.tableType == common::TableType::NODE;
    });
  }
  bool hasRelInfo() const {
    return hasInfo([](const BoundSetPropertyInfo& info) {
      return info.tableType == common::TableType::REL;
    });
  }
  std::vector<BoundSetPropertyInfo> getRelInfos() const {
    return getInfos([](const BoundSetPropertyInfo& info) {
      return info.tableType == common::TableType::REL;
    });
  }

 private:
  bool hasInfo(
      const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
  std::vector<BoundSetPropertyInfo> getInfos(
      const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

 private:
  std::vector<BoundSetPropertyInfo> infos;
};

}  // namespace binder
}  // namespace neug
