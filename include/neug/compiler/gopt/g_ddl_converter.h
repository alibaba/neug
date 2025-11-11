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
#include "neug/compiler/gopt/g_type_converter.h"
#include "neug/compiler/planner/operator/ddl/logical_alter.h"
#include "neug/compiler/planner/operator/ddl/logical_create_table.h"
#include "neug/compiler/planner/operator/ddl/logical_drop.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/cypher_ddl.pb.h"

namespace gs {
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
  explicit GDDLConverter(gs::catalog::Catalog* catalog) : catalog{catalog} {}

  virtual ~GDDLConverter() = default;

  // Convert LogicalCreateTable to DDLPlan
  std::unique_ptr<::physical::DDLPlan> convertCreateTable(
      const planner::LogicalCreateTable& op);

  // Convert LogicalDrop to DDLPlan
  std::unique_ptr<::physical::DDLPlan> convertDropTable(
      const planner::LogicalDrop& op);

  // Convert LogicalAlter to DDLPlan
  std::unique_ptr<::physical::DDLPlan> convertAlterTable(
      const planner::LogicalAlter& op);

  std::unique_ptr<::physical::DDLPlan> convert(
      const planner::LogicalPlan& plan);

 private:
  gs::catalog::Catalog* catalog;

  void checkCatalogInitialized() const {
    if (!catalog) {
      THROW_RUNTIME_ERROR("Catalog is not initialized.");
    }
  }

  // Convert to specific DDL operations
  std::unique_ptr<::physical::DDLPlan> convertToCreateVertexSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::DDLPlan> convertToCreateEdgeSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::DDLPlan> convertToCreateEdgeGroupSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::DDLPlan> convertToDropVertexSchema(
      const planner::LogicalDrop& op);
  std::unique_ptr<::physical::DDLPlan> convertToDropEdgeSchema(
      const planner::LogicalDrop& op);
  std::unique_ptr<::physical::DDLPlan> convertToAddVertexPropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToAddEdgePropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToDropVertexPropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToDropEdgePropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToRenameVertexPropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToRenameEdgePropertySchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToRenameVertexTypeSchema(
      const planner::LogicalAlter& op);
  std::unique_ptr<::physical::DDLPlan> convertToRenameEdgeTypeSchema(
      const planner::LogicalAlter& op);

  void getEdgeLabels(const std::string& labelName,
                     std::vector<EdgeLabel>& edgeLabels);
  std::string getVertexLabelName(gs::common::oid_t tableId);
  bool checkEntryType(const std::string& labelName,
                      catalog::CatalogEntryType expectedType);
  std::unique_ptr<::physical::EdgeType> convertToEdgeType(
      const EdgeLabel& label);
  std::unique_ptr<::physical::CreateEdgeSchema::TypeInfo> convertToEdgeTypeInfo(
      const binder::BoundCreateTableInfo& info, const std::string& edgeName);

 private:
  gs::gopt::GTypeConverter typeConverter;
};

}  // namespace gopt
}  // namespace gs