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

#include <google/protobuf/wrappers.pb.h>
#include <memory>
#include <unordered_set>
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/type.pb.h"

namespace neug {
namespace gopt {

struct GRelType;
struct GNodeType;

class GPhysicalTypeConverter {
 public:
  GPhysicalTypeConverter() = default;

  // convert logical type to physical type
  std::unique_ptr<::common::IrDataType> convertNodeType(
      const GNodeType& nodeType);
  std::unique_ptr<::common::IrDataType> convertRelType(const GRelType& relType);
  std::unique_ptr<::common::IrDataType> convertPathType(
      const GRelType& relType);

  std::unique_ptr<::common::IrDataType> convertArrayType(
      const common::LogicalType& type);
  std::unique_ptr<::common::IrDataType> convertStructType(
      const common::LogicalType& type);

  std::unique_ptr<::common::IrDataType> convertSimpleLogicalType(
      const common::LogicalType& type);

  std::unique_ptr<::common::IrDataType> convertLogicalType(
      const common::LogicalType& type);

 private:
  std::unique_ptr<::common::GraphDataType::GraphElementType> convertNodeTable(
      catalog::NodeTableCatalogEntry* nodeTable);
  std::unique_ptr<::common::GraphDataType::GraphElementType> convertRelTable(
      catalog::GRelTableCatalogEntry* relTable);
};

class GLogicalTypeConverter {
 public:
  GLogicalTypeConverter() = default;

  // convert physical type to logical type
  common::LogicalType convertDataType(const ::common::DataType& type);
};

}  // namespace gopt
}  // namespace neug
