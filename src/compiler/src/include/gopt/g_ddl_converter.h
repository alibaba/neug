#pragma once

#include <memory>
#include <string>
#include "catalog/catalog_entry/catalog_entry_type.h"
#include "gopt/g_catalog.h"
#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_drop.h"
#include "planner/operator/logical_plan.h"
// #include "src/proto_generated_gie/common.pb.h"
#include "gopt/g_type_converter.h"
#include "src/proto_generated_gie/common.pb.h"
#include "src/proto_generated_gie/cypher_ddl.pb.h"

namespace kuzu {
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
  explicit GDDLConverter(kuzu::catalog::Catalog* catalog) : catalog{catalog} {}

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
  kuzu::catalog::Catalog* catalog;

  void checkCatalogInitialized() const {
    if (!catalog) {
      throw std::runtime_error("Catalog is not initialized.");
    }
  }

  // Convert to specific DDL operations
  std::unique_ptr<::physical::DDLPlan> convertToCreateVertexSchema(
      const planner::LogicalCreateTable& op);
  std::unique_ptr<::physical::DDLPlan> convertToCreateEdgeSchema(
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
  std::string getVertexLabelName(kuzu::common::oid_t tableId);
  bool checkEntryType(const std::string& labelName,
                      catalog::CatalogEntryType expectedType);
  std::unique_ptr<::physical::EdgeType> convertToEdgeType(
      const EdgeLabel& label);

 private:
  kuzu::gopt::GTypeConverter typeConverter;
};

}  // namespace gopt
}  // namespace kuzu