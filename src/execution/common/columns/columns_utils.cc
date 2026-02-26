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

#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/list_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"

namespace neug {
namespace execution {
std::shared_ptr<IContextColumnBuilder> ColumnsUtils::create_builder(
    const DataType& type) {
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return std::make_shared<ValueColumnBuilder<type>>();
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVertex: {
    return std::make_shared<MLVertexColumnBuilder>();
  }
  case DataTypeId::kEdge: {
    return std::make_shared<BDMLEdgeColumnBuilder>(std::vector<LabelTriplet>());
  }
  case DataTypeId::kStruct: {
    return std::make_shared<StructColumnBuilder>(type);
  }
  case DataTypeId::kList: {
    DataType elem_type = ListType::GetChildType(type);
    return std::make_shared<ListColumnBuilder>(elem_type);
  }
  case DataTypeId::kPath: {
    return std::make_shared<PathColumnBuilder>();
  }
  // TODO: support null
  case DataTypeId::kNull: {
    return std::make_shared<ValueColumnBuilder<std::string>>();
  }
  default:
    LOG(FATAL) << "Unsupported data type for column builder: "
               << static_cast<int>(type.id());
    return nullptr;
  }
}
}  // namespace execution
}  // namespace neug