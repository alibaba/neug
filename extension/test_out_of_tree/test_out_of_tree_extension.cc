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

#include "neug/common/columns/value_columns.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/params_map.h"
#include "neug/utils/exception/exception.h"

namespace {

// String literal, or $param (ParamsMap key without '$').
struct DeferredCallArg {
  bool is_param = false;
  std::string param_name;
  std::string literal;

  static DeferredCallArg FromLiteral(std::string value) {
    DeferredCallArg arg;
    arg.literal = std::move(value);
    return arg;
  }

  static DeferredCallArg FromParam(std::string name) {
    DeferredCallArg arg;
    arg.is_param = true;
    arg.param_name = std::move(name);
    return arg;
  }

  std::string resolve(const neug::execution::ParamsMap& params) const {
    if (!is_param) {
      return literal;
    }
    auto it = params.find(param_name);
    if (it == params.end() || it->second.IsNull()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Missing parameter for TEST_ECHO_PARAM: " + param_name);
    }
    return it->second.GetValue<std::string>();
  }
};

struct EchoParamFuncInput : public neug::function::CallFuncInputBase {
  DeferredCallArg arg;
  std::string value;

  std::unique_ptr<neug::function::CallFuncInputBase> bindParams(
      const neug::execution::ParamsMap& params) const override {
    auto bound = std::make_unique<EchoParamFuncInput>(*this);
    bound->value = arg.resolve(params);
    return bound;
  }
};

struct TestEchoParamFunction {
  static constexpr const char* name = "TEST_ECHO_PARAM";

  static neug::function::function_set getFunctionSet() {
    auto func = std::make_unique<neug::function::NeugCallFunction>(
        name,
        neug::function::call_input_types{
            neug::common::DataType(neug::common::DataTypeId::kVarchar)},
        neug::function::call_output_columns{
            {"value",
             neug::common::DataType(neug::common::DataTypeId::kVarchar)}});

    func->bindFunc =
        [](const neug::Schema&, const neug::execution::ContextMeta&,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<neug::function::CallFuncInputBase> {
      const auto& procedure = plan.plan(op_idx).opr().procedure_call();
      if (procedure.query().arguments_size() < 1) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "TEST_ECHO_PARAM requires 1 VARCHAR argument");
      }
      auto input = std::make_unique<EchoParamFuncInput>();
      const auto& arg = procedure.query().arguments(0);
      if (arg.has_param()) {
        input->arg = DeferredCallArg::FromParam(arg.param().name());
      } else if (arg.has_const_() && arg.const_().has_str()) {
        input->arg = DeferredCallArg::FromLiteral(arg.const_().str());
      } else {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "TEST_ECHO_PARAM requires a string constant or $param");
      }
      return input;
    };

    func->execFunc = [](const neug::function::CallFuncInputBase& input_base,
                        neug::IStorageInterface&) {
      const auto& input = static_cast<const EchoParamFuncInput&>(input_base);

      neug::ValueColumnBuilder<std::string> builder;
      builder.reserve(1);
      builder.push_back_opt(input.value);

      neug::execution::Context ctx;
      neug::DataChunk chunk;
      chunk.set(0, builder.finish());
      ctx.append_chunk(std::move(chunk));
      ctx.tag_ids = {0};
      return ctx;
    };

    neug::function::function_set set;
    set.push_back(std::move(func));
    return set;
  }
};

}  // namespace

extern "C" {

void Init() {
  neug::extension::ExtensionAPI::registerFunction<TestEchoParamFunction>(
      neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          "test_out_of_tree",
          "Test extension for out-of-tree builds and CALL $params "
          "(TEST_ECHO_PARAM)."});
}

const char* Name() { return "TEST_OUT_OF_TREE"; }

}  // extern "C"
