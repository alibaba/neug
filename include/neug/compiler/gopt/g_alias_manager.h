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
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace gopt {

const static common::alias_id_t DEFAULT_ALIAS_ID = -1;
const static std::string DEFAULT_ALIAS_NAME = "__default_alias_name__";

class GAliasManager {
 public:
  GAliasManager(const planner::LogicalPlan& plan);

  common::alias_id_t getAliasId(const std::string& uniqueName);
  gopt::GAliasName getGAliasName(common::alias_id_t aliasId);

  static void extractGAliasNames(const planner::LogicalOperator& op,
                                 std::vector<gopt::GAliasName>& aliasNames);
  void extractAliasIds(const planner::LogicalOperator& op,
                       std::vector<common::alias_id_t>& aliasIds);
  std::string printForDebug();

 private:
  std::unordered_map<std::string, common::alias_id_t> uniqueNameToId;
  std::unordered_map<common::alias_id_t, gopt::GAliasName> idToGName;
  // std::unordered_map<common::alias_id_t, std::string> idToName;
  common::alias_id_t nextId{0};

 private:
  void visitOperator(const planner::LogicalOperator& op,
                     std::unordered_set<std::string>& vTags);
  // void addAliasName(const std::string& name);
  void addGAliasName(const gopt::GAliasName& gAliasName);
  static std::vector<gopt::GAliasName> extractSingleOpGAliasNames(
      const planner::LogicalOperator& op);
};

}  // namespace gopt
}  // namespace neug
