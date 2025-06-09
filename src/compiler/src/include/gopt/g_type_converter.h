// this class is used to convert kuzu DataType to physical metadata
// here have 3 more functions:
// IrDataType convertNodeType(const GNodeType &nodeType);
// IrDataType convertRelType(const GRelType &relType);
// IrDataType convertBasicType(const common::LogicalType& type);
#pragma once

#include <google/protobuf/wrappers.pb.h>
#include <memory>
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "gopt/g_graph_type.h"
#include "src/proto_generated_gie/basic_type.pb.h"
#include "src/proto_generated_gie/type.pb.h"

namespace kuzu {
namespace gopt {

struct GRelType;
struct GNodeType;

class GTypeConverter {
 public:
  GTypeConverter() = default;

  std::unique_ptr<::common::IrDataType> convertNodeType(
      const GNodeType& nodeType);
  std::unique_ptr<::common::IrDataType> convertRelType(const GRelType& relType);
  std::unique_ptr<::common::IrDataType> convertLogicalType(
      const common::LogicalType& type);
  std::unique_ptr<::common::IrDataType> convertType(
      const common::LogicalType& type, const binder::Expression& expr);

 private:
  std::unique_ptr<::common::GraphDataType::GraphElementType> convertNodeTable(
      catalog::NodeTableCatalogEntry* nodeTable);
  std::unique_ptr<::common::GraphDataType::GraphElementType> convertRelTable(
      catalog::GRelTableCatalogEntry* relTable);
};

}  // namespace gopt
}  // namespace kuzu
