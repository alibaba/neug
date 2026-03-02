/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/compiler/function/show_loaded_extensions_function.h"
#include <glog/logging.h>
#include "neug/compiler/extension/extension_api.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

function_set ShowLoadedExtensionsFunction::getFunctionSet() {
  auto function = std::make_unique<NeugCallFunction>(
      ShowLoadedExtensionsFunction::name,
      std::vector<neug::common::LogicalTypeID>{},
      std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
          {"name", neug::common::LogicalTypeID::STRING},
          {"description", common::LogicalTypeID::STRING}});

  function->bindFunc = [](const neug::Schema& schema,
    const neug::execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan,
    int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    return std::make_unique<ShowLoadedExtensionsFuncInput>();
  };

  function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
    try {
      neug::execution::Context ctx;
      const auto& ext_map = neug::extension::ExtensionAPI::getLoadedExtensions();
  
      neug::execution::ValueColumnBuilder<std::string> name_builder;
      neug::execution::ValueColumnBuilder<std::string> desc_builder;
      name_builder.reserve(ext_map.size());
      desc_builder.reserve(ext_map.size());
  
      for (const auto& kv : ext_map) {
        const std::string& name_view = kv.second.name;
        name_builder.push_back_opt(name_view);
  
        const std::string& desc_view = kv.second.description;
        desc_builder.push_back_opt(desc_view);
      }
  
      ctx.set(0, name_builder.finish());
      ctx.set(1, desc_builder.finish());
      ctx.tag_ids = {0, 1};
      return ctx;
    } catch (const std::exception& e) {
      LOG(ERROR) << "ShowLoadedExtensions failed: " << e.what();
      THROW_EXTENSION_EXCEPTION("ShowLoadedExtensions failed: " +
                                std::string(e.what()));
    }
  };

  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace neug