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

#include <string>
#include <tuple>  // for tuple
#include <vector>

#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/basic_type.pb.h"  // for DataType (ptr only)
#include "neug/utils/proto/plan/cypher_ddl.pb.h"
#include "neug/utils/proto/plan/physical.pb.h"
#endif
#include "neug/utils/result.h"

namespace common {
class Value;
}  // namespace common

namespace gs {

// Utility functions for parsing and converting Protocol Buffers (protobuf)
// messages

Any get_default_value(const PropertyType& type);

bool multiplicity_to_storage_strategy(
    const physical::CreateEdgeSchema::Multiplicity& multiplicity,
    EdgeStrategy& oe_strategy, EdgeStrategy& ie_strategy);

bool primitive_type_to_property_type(
    const ::common::PrimitiveType& primitive_type, PropertyType& out_type);

bool string_type_to_property_type(const ::common::String& string_type,
                                  PropertyType& out_type);

bool temporal_type_to_property_type(const ::common::Temporal& temporal_type,
                                    PropertyType& out_type);

bool data_type_to_property_type(const ::common::DataType& data_type,
                                PropertyType& out_type);

bool common_value_to_any(const ::common::Value& value, Any& out_any);

Result<std::vector<std::tuple<PropertyType, std::string, Any>>>
property_defs_to_tuple(
    const google::protobuf::RepeatedPtrField<physical::PropertyDef>&
        properties);

// Convert to a bool representing error_on_conflict.
bool conflict_action_to_bool(const physical::ConflictAction& action);

// Currently support
// 1. common::Value const
// 2. ToDate to_date
// 3. ToDatetime to_datetime
// 4. ToInterval to_interval
Any expr_opr_value_to_any(const ::common::ExprOpr& value);
}  // namespace gs
