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

#include <memory>
#include <string>
#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_expr_converter.h"
#include "neug/compiler/gopt/g_type_converter.h"
#include "neug/compiler/planner/operator/ddl/logical_alter.h"
#include "neug/compiler/planner/operator/ddl/logical_create_table.h"
#include "neug/compiler/planner/operator/ddl/logical_drop.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace neug {
namespace gopt {

struct EdgeLabel {
  std::string typeName;
  std::string srcTypeName;
  std::string dstTypeName;

  EdgeLabel(const std::string& type, const std::string& src,
            const std::string& dst)
      : typeName(type), srcTypeName(src), dstTypeName(dst) {}
};

class GDDLConverter {
 public:
  explicit GDDLConverter(std::shared_ptr<GAliasManager> aliasManager,
                         neug::catalog::Catalog* catalog)
      : catalog{catalog}, exprConverter(aliasManager) {}

  virtual ~GDDLConverter() = default;

  // Convert LogicalCreateTable to PhysicalPlan
  void convertCreateTable(const planner::LogicalCreateTable& op,
                          ::physical::PhysicalPlan* plan);

  // Convert LogicalDrop to PhysicalPlan
  void convertDropTable(const planner::LogicalDrop& op,
                        ::physical::PhysicalPlan* plan);

  // Convert LogicalAlter to PhysicalPlan
  void convertAlterTable(const planner::LogicalAlter& op,
                         ::physical::PhysicalPlan* plan);

 private:
  neug::catalog::Catalog* catalog;

  void checkCatalogInitialized() const {
    if (!catalog) {
      THROW_RUNTIME_ERROR("Catalog is not initialized.");
    }
  }

  // Convert to specific DDL operations
  std::unique_ptr<::physical::PhysicalOpr> convertToCreateVertexSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToCreateEdgeSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToCreateEdgeGroupSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToDropVertexSchema(
      const planner::LogicalDrop& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToDropEdgeSchema(
      const planner::LogicalDrop& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToAddVertexPropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToAddEdgePropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToDropVertexPropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToDropEdgePropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToRenameVertexPropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToRenameEdgePropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToRenameVertexTypeSchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::PhysicalOpr> convertToRenameEdgeTypeSchema(
      const planner::LogicalAlter& op);

  void getEdgeLabels(const std::string& labelName,
                     std::vector<EdgeLabel>& edgeLabels);
  std::string getVertexLabelName(neug::common::oid_t tableId);
  bool checkEntryType(const std::string& labelName,
                      catalog::CatalogEntryType expectedType);
  std::unique_ptr<::physical::EdgeType> convertToEdgeType(
      const EdgeLabel& label);
  std::unique_ptr<::physical::CreateEdgeSchema::TypeInfo> convertToEdgeTypeInfo(
      const binder::BoundCreateTableInfo& info, const std::string& edgeName);

 private:
  neug::gopt::GPhysicalTypeConverter typeConverter;
  neug::gopt::GExprConverter exprConverter;
};

}  // namespace gopt
}  // namespace neug