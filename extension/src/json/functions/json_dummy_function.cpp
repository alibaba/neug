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

#include "json/json_dummy_function.h"
#include <glog/logging.h>
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::function;
using namespace neug::runtime;

namespace neug {
namespace extension {

function_set JsonDummyFunction::getFunctionSet() {
  neug::function::function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, JsonDummyFunction::Exec));
  return functionSet;
}

runtime::Value JsonDummyFunction::Exec(
    const std::vector<runtime::Value>& args) {
  if (args.size() != 1) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "JSON_DUMMY: expect exactly 1 argument, got " +
        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_EXCEPTION_WITH_FILE_LINE("JSON_DUMMY: input value is not a string");
  }

  std::string s(val.GetValue<std::string>());
  LOG(INFO) << "[json extension] JSON_DUMMY Exec called, arg='" << s << "'";

  return runtime::Value::STRING(s);
}

}  // namespace extension
}  // namespace neug