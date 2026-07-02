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

#include "neug/columnar/columns/columns_utils.h"
#include "neug/columnar/columns/edge_columns.h"
#include "neug/columnar/columns/i_column.h"
#include "neug/columnar/columns/list_columns.h"
#include "neug/columnar/columns/path_columns.h"
#include "neug/columnar/columns/struct_columns.h"
#include "neug/columnar/columns/value_columns.h"
#include "neug/columnar/columns/vertex_columns.h"
#include "neug/columnar/data_chunk.h"
#include "neug/columnar/graph_types.h"
#include "neug/columnar/value.h"

namespace neug {
namespace execution {

#include "neug/columnar/graph_types.h"
using columnar::decode_edge_label_id;
using columnar::decode_unique_vertex_id;
using columnar::encode_unique_edge_id;
using columnar::encode_unique_vertex_id;
using columnar::foreach_vertex;
using columnar::generate_edge_label_id;

using columnar::AggrKind;
using columnar::BDMLEdgeColumn;
using columnar::BDMLEdgeColumnBuilder;
using columnar::BDSLEdgeColumn;
using columnar::BDSLEdgeColumnBuilder;
using columnar::ColumnKind;
using columnar::ColumnsUtils;
using columnar::DataChunk;
using columnar::date_t;
using columnar::Direction;
using columnar::edge_t;
using columnar::EdgeColumnType;
using columnar::EdgeRecord;
using columnar::IColumn;
using columnar::IColumnBuilder;
using columnar::IEdgeColumn;
using columnar::interval_t;
using columnar::IVertexColumn;
using columnar::IVertexColumnBuilder;
using columnar::JoinKind;
using columnar::LabelTriplet;
using columnar::ListColumn;
using columnar::ListColumnBuilder;
using columnar::ListValue;
using columnar::MLVertexColumn;
using columnar::MLVertexColumnBuilder;
using columnar::MLVertexColumnBuilderOpt;
using columnar::MSEdgeColumn;
using columnar::MSEdgeColumnBuilder;
using columnar::MSVertexColumn;
using columnar::MSVertexColumnBuilder;
using columnar::Path;
using columnar::PathColumn;
using columnar::PathColumnBuilder;
using columnar::PathOpt;
using columnar::PathValue;
using columnar::SDMLEdgeColumn;
using columnar::SDMLEdgeColumnBuilder;
using columnar::SDSLEdgeColumn;
using columnar::SDSLEdgeColumnBuilder;
using columnar::SLVertexColumn;
using columnar::StringValue;
using columnar::StructColumn;
using columnar::StructColumnBuilder;
using columnar::StructValue;
using columnar::timestamp_ms_t;
using columnar::Value;
using columnar::ValueConverter;
using columnar::vertex_t;
using columnar::VertexColumnType;
using columnar::VertexRecord;
using columnar::VOpt;

template <typename T>
using ValueColumn = columnar::ValueColumn<T>;

template <typename T>
using ValueColumnBuilder = columnar::ValueColumnBuilder<T>;

}  // namespace execution
}  // namespace neug
