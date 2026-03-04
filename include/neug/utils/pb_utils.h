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

#include <google/protobuf/util/json_util.h>
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

// Helper function to set up JsonPrintOptions with compatibility across protobuf
// versions
inline void configure_json_print_options_for_all_fields(
    google::protobuf::util::JsonPrintOptions& options) {
#if PROTOBUF_VERSION < \
    4026000  // Before v26.0 where always_print_primitive_fields was removed
  options.always_print_primitive_fields = true;
#else
  options.always_print_fields_with_no_presence = true;  // Replacement field
#endif
}

template <typename T>
std::string proto_to_string(const T& proto) {
  std::string json_str;
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  configure_json_print_options_for_all_fields(options);
  auto status =
      google::protobuf::util::MessageToJsonString(proto, &json_str, options);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to convert proto to string: " +
                        status.ToString());
  }
  return json_str;
}

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
