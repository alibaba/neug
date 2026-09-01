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

#include "gopt_test.h"

#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/retrieve/union.h"
#include "neug/execution/execute/plan_parser.h"

namespace neug {
namespace gopt {

namespace {

int findUnionOperator(const physical::PhysicalPlan& plan) {
  for (int i = 0; i < plan.plan_size(); ++i) {
    if (plan.plan(i).opr().has_union_()) {
      return i;
    }
  }
  return -1;
}

physical::Project* findLastProject(physical::PhysicalPlan* plan) {
  for (int i = plan->plan_size() - 1; i >= 0; --i) {
    auto* opr = plan->mutable_plan(i)->mutable_opr();
    if (opr->has_project()) {
      return opr->mutable_project();
    }
  }
  return nullptr;
}

class UnionMetaTest : public GOptTest {
 public:
  static void SetUpTestSuite() { execution::PlanParser::get().init(); }
};

TEST_F(UnionMetaTest, ReturnsFirstBranchOutputMeta) {
  auto logical =
      planLogical("RETURN 1 AS id LIMIT 1 UNION ALL RETURN 2 AS id LIMIT 1");
  auto physical = planPhysical(*logical);
  const int union_idx = findUnionOperator(*physical);
  ASSERT_GE(union_idx, 0);

  auto* union_op =
      physical->mutable_plan(union_idx)->mutable_opr()->mutable_union_();
  ASSERT_EQ(union_op->sub_plans_size(), 2);
  auto* first_project = findLastProject(union_op->mutable_sub_plans(0));
  ASSERT_NE(first_project, nullptr);
  ASSERT_EQ(first_project->mappings_size(), 1);
  ASSERT_TRUE(first_project->mappings(0).has_alias());
  const int expected_alias = first_project->mappings(0).alias().value();

  execution::ops::UnionOprBuilder builder;
  auto result = builder.Build(currentSchema, execution::ContextMeta(),
                              *physical, union_idx);

  ASSERT_TRUE(result) << result.error().ToString();
  ASSERT_NE(result.value().first, nullptr);
  EXPECT_EQ(result.value().second.columns().size(), 1);
  EXPECT_TRUE(result.value().second.exist(expected_alias));
}

TEST_F(UnionMetaTest, RejectsDifferentBranchOutputAliases) {
  auto logical =
      planLogical("RETURN 1 AS id LIMIT 1 UNION ALL RETURN 2 AS id LIMIT 1");
  auto physical = planPhysical(*logical);
  const int union_idx = findUnionOperator(*physical);
  ASSERT_GE(union_idx, 0);

  auto* union_op =
      physical->mutable_plan(union_idx)->mutable_opr()->mutable_union_();
  ASSERT_EQ(union_op->sub_plans_size(), 2);
  auto* first_project = findLastProject(union_op->mutable_sub_plans(0));
  auto* second_project = findLastProject(union_op->mutable_sub_plans(1));
  ASSERT_NE(first_project, nullptr);
  ASSERT_NE(second_project, nullptr);
  ASSERT_EQ(first_project->mappings_size(), 1);
  ASSERT_EQ(second_project->mappings_size(), 1);
  ASSERT_TRUE(first_project->mappings(0).has_alias());
  ASSERT_TRUE(second_project->mappings(0).has_alias());

  const int first_alias = first_project->mappings(0).alias().value();
  second_project->mutable_mappings(0)->mutable_alias()->set_value(first_alias +
                                                                  1);

  execution::ops::UnionOprBuilder builder;
  auto result = builder.Build(currentSchema, execution::ContextMeta(),
                              *physical, union_idx);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_SCHEMA_MISMATCH);
  EXPECT_NE(result.error().error_message().find(
                "output aliases of branch 1 do not match branch 0"),
            std::string::npos);
}

TEST_F(UnionMetaTest, RejectsMoreThanTwoBranchesDuringConversion) {
  auto logical = planLogical(
      "RETURN 1 AS id UNION ALL RETURN 2 AS id UNION ALL RETURN 3 AS id");

  try {
    auto physical = planPhysical(*logical);
    FAIL() << "Expected conversion to reject an N-ary UNION ALL";
  } catch (const exception::NotSupportedException& e) {
    EXPECT_NE(std::string(e.what()).find(
                  "UNION ALL currently supports exactly two branches; got 3"),
              std::string::npos);
  }
}

TEST_F(UnionMetaTest, RejectsPhysicalPlanWithMoreThanTwoSubPlans) {
  auto logical = planLogical("RETURN 1 AS id UNION ALL RETURN 2 AS id");
  auto physical = planPhysical(*logical);
  const int union_idx = findUnionOperator(*physical);
  ASSERT_GE(union_idx, 0);

  auto* union_op =
      physical->mutable_plan(union_idx)->mutable_opr()->mutable_union_();
  ASSERT_EQ(union_op->sub_plans_size(), 2);
  physical::PhysicalPlan third_sub_plan = union_op->sub_plans(0);
  union_op->add_sub_plans()->CopyFrom(third_sub_plan);

  execution::ops::UnionOprBuilder builder;
  auto result = builder.Build(currentSchema, execution::ContextMeta(),
                              *physical, union_idx);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().error_code(), neug::StatusCode::ERR_NOT_SUPPORTED);
  EXPECT_NE(result.error().error_message().find(
                "exactly two sub-plans are supported, got 3"),
            std::string::npos);
}

}  // namespace

}  // namespace gopt
}  // namespace neug
