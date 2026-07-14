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

#include <gtest/gtest.h>
#include <filesystem>

#include "neug/common/columns/value_columns.h"
#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/params_map.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"

namespace neug {
namespace test {
namespace {

// Typed literal Value, or $param (ParamsMap key without '$').
struct DeferredCallArg {
  bool is_param = false;
  std::string param_name;
  Value literal;

  static DeferredCallArg FromLiteral(Value value) {
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

  Value resolve(const execution::ParamsMap& params) const {
    if (!is_param) {
      return literal;
    }
    auto it = params.find(param_name);
    if (it == params.end()) {
      return Value();
    }
    return it->second;
  }
};

struct EchoParamFuncInput : public function::CallFuncInputBase {
  DeferredCallArg arg;
  Value value;

  void evaluateParams(const execution::ParamsMap& params) override {
    value = arg.resolve(params);
  }
};

struct TestEchoParamFunction {
  static constexpr const char* name = "TEST_ECHO_PARAM";

  static function::function_set getFunctionSet() {
    auto func = std::make_unique<function::NeugCallFunction>(
        name,
        function::call_input_types{
            common::DataType(common::DataTypeId::kVarchar)},
        function::call_output_columns{
            {"value", common::DataType(common::DataTypeId::kVarchar)}});

    func->bindFunc =
        [](const Schema&, const execution::ContextMeta&,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
      auto input = std::make_unique<EchoParamFuncInput>();
      const auto& procedure = plan.plan(op_idx).opr().procedure_call();
      if (procedure.query().arguments_size() < 1) {
        return input;
      }
      const auto& arg = procedure.query().arguments(0);
      if (!arg.param_name().empty()) {
        input->arg = DeferredCallArg::FromParam(arg.param_name());
      } else if (arg.has_const_() && arg.const_().has_str()) {
        input->arg =
            DeferredCallArg::FromLiteral(Value::STRING(arg.const_().str()));
      }
      return input;
    };

    func->execFunc = [](const function::CallFuncInputBase& input_base,
                        IStorageInterface&) {
      const auto& input = static_cast<const EchoParamFuncInput&>(input_base);
      std::string out;
      if (!input.value.IsNull()) {
        out = input.value.GetValue<std::string>();
      }

      ValueColumnBuilder<std::string> builder;
      builder.reserve(1);
      builder.push_back_opt(out);

      execution::Context ctx;
      DataChunk chunk;
      chunk.set(0, builder.finish());
      ctx.append_chunk(std::move(chunk));
      ctx.tag_ids = {0};
      return ctx;
    };

    function::function_set set;
    set.push_back(std::move(func));
    return set;
  }
};

}  // namespace

class CallParamsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ =
        std::filesystem::temp_directory_path() / "neug_call_params_test";
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);

    db_ = std::make_unique<NeugDB>();
    NeugDBConfig config;
    config.data_dir = (test_dir_ / "graph").string();
    config.checkpoint_on_close = false;
    db_->Open(config);

    extension::ExtensionAPI::registerFunction<TestEchoParamFunction>(
        catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  }

  void TearDown() override {
    if (db_ && !db_->IsClosed()) {
      db_->Close();
    }
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::filesystem::path test_dir_;
  std::unique_ptr<NeugDB> db_;
};

TEST_F(CallParamsTest, ExecFuncReceivesParamsMap) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  // Cypher uses $param; ParamsMap key omits the '$'.
  execution::ParamsMap params;
  params.emplace("param", Value::STRING("hello-from-params"));

  auto res = conn->Query("CALL TEST_ECHO_PARAM($param) RETURN value;", "update",
                         params);
  ASSERT_TRUE(res) << res.error().ToString();

  auto& result = res.value();
  ASSERT_TRUE(result.hasNext());
  EXPECT_EQ(result.GetString(0), "hello-from-params");
  EXPECT_EQ(result.GetString("value"), "hello-from-params");
}

TEST_F(CallParamsTest, MissingParamYieldsEmptyValue) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  auto res = conn->Query("CALL TEST_ECHO_PARAM($param) RETURN value;", "update",
                         execution::ParamsMap{});
  ASSERT_TRUE(res) << res.error().ToString();

  auto& result = res.value();
  ASSERT_TRUE(result.hasNext());
  EXPECT_EQ(result.GetString(0), "");
}

TEST_F(CallParamsTest, LiteralArgumentStillWorks) {
  auto conn = db_->Connect();
  ASSERT_NE(conn, nullptr);

  auto res =
      conn->Query("CALL TEST_ECHO_PARAM('literal-ok') RETURN value;", "update");
  ASSERT_TRUE(res) << res.error().ToString();

  auto& result = res.value();
  ASSERT_TRUE(result.hasNext());
  EXPECT_EQ(result.GetString(0), "literal-ok");
}

}  // namespace test
}  // namespace neug
