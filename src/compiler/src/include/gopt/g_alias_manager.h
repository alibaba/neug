#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include "src/compiler/src/include/common/types/types.h"
#include "src/compiler/src/include/planner/operator/logical_operator.h"
#include "src/compiler/src/include/planner/operator/logical_plan.h"

namespace gs {
namespace gopt {

class GAliasManager {
 public:
  GAliasManager(const planner::LogicalPlan& plan);

  common::alias_id_t getAliasId(const std::string& name);
  std::string getAliasName(common::alias_id_t id);
  std::string printForDebug();

  static void extractAliasName(const planner::LogicalOperator& op,
                               std::vector<std::string>& aliasNames);

 private:
  std::unordered_map<std::string, common::alias_id_t> nameToId;
  std::unordered_map<common::alias_id_t, std::string> idToName;
  common::alias_id_t nextId{0};

 private:
  void visitOperator(const planner::LogicalOperator& op);
  void addAliasName(const std::string& name);
};

}  // namespace gopt
}  // namespace gs
