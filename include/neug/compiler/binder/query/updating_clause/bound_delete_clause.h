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

#include "bound_delete_info.h"
#include "bound_updating_clause.h"

namespace neug {
namespace binder {

class BoundDeleteClause final : public BoundUpdatingClause {
 public:
  BoundDeleteClause() : BoundUpdatingClause{common::ClauseType::DELETE_} {};

  void addInfo(BoundDeleteInfo info) { infos.push_back(std::move(info)); }

  bool hasNodeInfo() const {
    return hasInfo([](const BoundDeleteInfo& info) {
      return info.tableType == common::TableType::NODE;
    });
  }
  std::vector<BoundDeleteInfo> getNodeInfos() const {
    return getInfos([](const BoundDeleteInfo& info) {
      return info.tableType == common::TableType::NODE;
    });
  }
  bool hasRelInfo() const {
    return hasInfo([](const BoundDeleteInfo& info) {
      return info.tableType == common::TableType::REL;
    });
  }
  std::vector<BoundDeleteInfo> getRelInfos() const {
    return getInfos([](const BoundDeleteInfo& info) {
      return info.tableType == common::TableType::REL;
    });
  }

 private:
  bool hasInfo(
      const std::function<bool(const BoundDeleteInfo& info)>& check) const;
  std::vector<BoundDeleteInfo> getInfos(
      const std::function<bool(const BoundDeleteInfo& info)>& check) const;

 private:
  std::vector<BoundDeleteInfo> infos;
};

}  // namespace binder
}  // namespace neug
