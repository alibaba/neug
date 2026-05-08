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

#include <google/protobuf/message.h>
#include <string>
#include <tuple>
#include <vector>

#include "neug/generated/proto/plan/basic_type.pb.h"
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

namespace common {
class Value;
}  // namespace common

namespace neug {
namespace execution {
class Value;
}

std::vector<std::string> parse_result_schema_column_names(
    const std::string& result_schema);

// Convert any protobuf Message to pretty-printed JSON string.
// This is a non-inline exported function so that test binaries do not need to
// link ${Protobuf_LIBRARIES} directly (the implementation lives inside
// libneug, avoiding protobuf descriptor double-registration).
NEUG_API std::string proto_to_string(const google::protobuf::Message& proto);

bool multiplicity_to_storage_strategy(
    const ::physical::CreateEdgeSchema::Multiplicity& multiplicity,
    EdgeStrategy& oe_strategy, EdgeStrategy& ie_strategy);

neug::result<std::vector<std::pair<std::string, execution::Value>>>
property_defs_to_value(
    const google::protobuf::RepeatedPtrField<::physical::PropertyDef>&
        properties);

// Convert to a bool representing error_on_conflict.
bool conflict_action_to_bool(const ::physical::ConflictAction& action);

}  // namespace neug
