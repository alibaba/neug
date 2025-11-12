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
#include "neug/compiler/extension/extension_api.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/utils/exception/exception.h"
#include <glog/logging.h>

namespace gs {
namespace function {

function_set ShowLoadedExtensionsFunction::getFunctionSet() {
  auto function = std::make_unique<NeugCallFunction>(
      ShowLoadedExtensionsFunction::name, 
      std::vector<gs::common::LogicalTypeID>{},
      std::vector<std::pair<std::string, gs::common::LogicalTypeID>>{
        {"name", gs::common::LogicalTypeID::STRING},
        {"description", common::LogicalTypeID::STRING}
      });

  function->bindFunc = [](const gs::Schema& schema, 
                         const gs::runtime::ContextMeta& ctx_meta,
                         const ::physical::PhysicalPlan& plan,
                         int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    return bindFunc(schema, ctx_meta, plan, op_idx);
  };
  
  function->execFunc = [](const CallFuncInputBase& input) -> gs::runtime::Context {
    const auto& showInput = static_cast<const ShowLoadedExtensionsFuncInput&>(input);
    return execFunc(showInput);
  };
  
  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

std::unique_ptr<ShowLoadedExtensionsFuncInput> ShowLoadedExtensionsFunction::bindFunc(
    const gs::Schema& schema,
    const gs::runtime::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan,
    int op_idx) {
  
  return std::make_unique<ShowLoadedExtensionsFuncInput>();
}

gs::runtime::Context ShowLoadedExtensionsFunction::execFunc(
    const ShowLoadedExtensionsFuncInput& input) {
  try {
    gs::runtime::Context ctx;
    const auto& ext_map = gs::extension::ExtensionAPI::getLoadedExtensions();

    gs::runtime::ValueColumnBuilder<std::string_view> name_builder;
    gs::runtime::ValueColumnBuilder<std::string_view> desc_builder;
    name_builder.reserve(ext_map.size());
    desc_builder.reserve(ext_map.size());

    for (const auto& kv : ext_map) {
      std::string_view name_view = kv.second.name;
      name_builder.push_back_opt(name_view);

      std::string_view desc_view = kv.second.description;
      desc_builder.push_back_opt(desc_view);
    }

    ctx.set(0, name_builder.finish());
    ctx.set(1, desc_builder.finish());
    ctx.tag_ids = {0, 1};
    return ctx;
  } catch (const std::exception& e) {
    LOG(ERROR) << "ShowLoadedExtensions failed: " << e.what();
    THROW_EXTENSION_EXCEPTION("ShowLoadedExtensions failed: "
                              + std::string(e.what()));
  }
}

}  // namespace function
}  // namespace gs