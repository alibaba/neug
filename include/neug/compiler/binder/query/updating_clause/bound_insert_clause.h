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

#include "bound_insert_info.h"
#include "bound_updating_clause.h"

namespace neug {
namespace binder {

class BoundInsertClause final : public BoundUpdatingClause {
 public:
  explicit BoundInsertClause(std::vector<BoundInsertInfo> infos)
      : BoundUpdatingClause{common::ClauseType::INSERT},
        infos{std::move(infos)} {}

  const std::vector<BoundInsertInfo>& getInfos() const { return infos; }

  bool hasNodeInfo() const {
    return hasInfo([](const BoundInsertInfo& info) {
      return info.tableType == common::TableType::NODE;
    });
  }
  std::vector<const BoundInsertInfo*> getNodeInfos() const {
    return getInfos([](const BoundInsertInfo& info) {
      return info.tableType == common::TableType::NODE;
    });
  }
  bool hasRelInfo() const {
    return hasInfo([](const BoundInsertInfo& info) {
      return info.tableType == common::TableType::REL;
    });
  }
  std::vector<const BoundInsertInfo*> getRelInfos() const {
    return getInfos([](const BoundInsertInfo& info) {
      return info.tableType == common::TableType::REL;
    });
  }

 private:
  bool hasInfo(
      const std::function<bool(const BoundInsertInfo& info)>& check) const;
  std::vector<const BoundInsertInfo*> getInfos(
      const std::function<bool(const BoundInsertInfo& info)>& check) const;

 private:
  std::vector<BoundInsertInfo> infos;
};

}  // namespace binder
}  // namespace neug
