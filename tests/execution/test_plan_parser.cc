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

#include <string>

#include "neug/execution/execute/plan_parser.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace neug::execution {
namespace {

::common::Expression parameter(const std::string& name,
                               ::common::PrimitiveType type) {
  ::common::Expression expression;
  auto* param = expression.add_operators()->mutable_param();
  param->set_name(name);
  param->mutable_data_type()->mutable_data_type()->set_primitive_type(type);
  return expression;
}

TEST(PlanParserTest, CollectsNestedParametersInSourceAndProjection) {
  ::common::Expression expression;
  auto* tuple = expression.add_operators()->mutable_to_tuple();
  auto* array = tuple->add_fields()->add_operators()->mutable_to_array();
  auto* list = array->add_fields()->add_operators()->mutable_to_list();
  auto* scalar = list->add_fields()->add_operators()->mutable_scalar_func();
  auto* cases = scalar->add_parameters()->add_operators()->mutable_case_();
  auto* when = cases->add_when_then_expressions();
  *when->mutable_when_expression() = parameter("enabled", ::common::DT_BOOL);
  *when->mutable_then_result_expression() =
      parameter("first", ::common::DT_SIGNED_INT64);
  *cases->mutable_else_result_expression() =
      parameter("last", ::common::DT_SIGNED_INT64);
  auto* udf = tuple->add_fields()->add_operators()->mutable_udf_func();
  *udf->add_parameters() = parameter("weight", ::common::DT_DOUBLE);
  *udf->add_parameters() = parameter("first", ::common::DT_SIGNED_INT64);

  const ParamsMetaMap expected{{"enabled", DataType::BOOLEAN},
                               {"first", DataType::INT64},
                               {"last", DataType::INT64},
                               {"weight", DataType::DOUBLE}};
  for (const bool source : {true, false}) {
    SCOPED_TRACE(source ? "source" : "projection");
    physical::PhysicalPlan plan;
    auto* op = plan.add_plan()->mutable_opr();
    if (source) {
      *op->mutable_source()->mutable_skip_rows() = expression;
    } else {
      *op->mutable_project()->add_mappings()->mutable_expr() = expression;
    }
    const auto original = plan.SerializeAsString();
    EXPECT_EQ(PlanParser::parse_params_type(plan), expected);
    EXPECT_EQ(plan.SerializeAsString(), original);
  }
}

TEST(PlanParserTest, CollectsIntervalParametersAndDeduplicatesNames) {
  physical::PhysicalPlan plan;
  auto* predicate =
      plan.add_plan()->mutable_opr()->mutable_source()->mutable_skip_rows();
  *predicate = parameter("count", ::common::DT_SIGNED_INT32);
  auto* interval = predicate->add_operators()->mutable_time_interval();
  *interval->mutable_param() =
      parameter("days", ::common::DT_SIGNED_INT64).operators(0).param();
  interval = predicate->add_operators()->mutable_time_interval();
  *interval->mutable_param() =
      parameter("days", ::common::DT_SIGNED_INT64).operators(0).param();

  const ParamsMetaMap expected{{"count", DataType::INT32},
                               {"days", DataType::INT64}};
  EXPECT_EQ(PlanParser::parse_params_type(plan), expected);
}

}  // namespace
}  // namespace neug::execution
