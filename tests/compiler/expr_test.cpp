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
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/planner/operator/logical_projection.h"

namespace neug {
namespace gopt {

class ExprTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");
  std::string statsData = "";
  std::string getExprResourcePath(std::string resource) {
    return getGOptResourcePath("expr_test/" + resource);
  };

  std::string getExprResource(std::string resource) {
    return getGOptResource("expr_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(ExprTest, IS_NULL) {
  std::string query = "MATCH (a:person) WHERE a.age IS NULL RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("IS_NULL_physical"));
}

TEST_F(ExprTest, IS_NOT_NULL) {
  std::string query =
      "MATCH (a:person) WHERE a.age IS NOT NULL RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("IS_NOT_NULL_physical"));
}

TEST_F(ExprTest, MULTI_EQUAL_1) {
  std::string query =
      "MATCH (a:person) WHERE a.gender * 2.1 = 2.1 * a.gender RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("MULTI_EQUAL_1_physical"));
}

TEST_F(ExprTest, MULTI_EQUAL_2) {
  std::string query =
      "MATCH (a:person) WHERE (a.gender*3.5 = 7.0) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("MULTI_EQUAL_2_physical"));
}

TEST_F(ExprTest, ADD_NULL) {
  // null is converted to 0 in arithmetic expressions
  std::string query =
      "MATCH (a:person) WHERE a.age + null <= 200 RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("ADD_NULL_physical"));
}

TEST_F(ExprTest, ORDER_BY_ADD) {
  // p.age + p.ID is calculated before ordering
  std::string query =
      "MATCH (p:person) RETURN p.ID, p.age ORDER BY p.age + p.ID;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("ORDER_BY_ADD_physical"));
}

TEST_F(ExprTest, TYPE_CAST) {
  // `a.gender` is i32 type, `1` is i64 type, here actually exists a
  // cast(a.gender, i64) function, but engine can support the implicit type
  // conversion among numeric types, so the cast is skipped.
  std::string query =
      "MATCH (a:person)-[:knows]-(b:person)-[:knows]-(c:person) "
      "WHERE a.gender = 1 AND b.gender = 2 AND c.fName = \"Bob\" "
      "RETURN a.fName, b.fName;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("TYPE_CAST_physical"));
}

TEST_F(ExprTest, SUM_AGE) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) "
      "WHERE a.age < 31 RETURN a.age + b.age + c.age;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("SUM_AGE_physical"));
}

TEST_F(ExprTest, MULTI_EQUAL_3) {
  std::string query =
      "MATCH (a:person) WHERE (a.gender*3.5 = 7.0) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("MULTI_EQUAL_3_physical"));
}

TEST_F(ExprTest, PROJECT_DATE) {
  std::string query = "MATCH (a:person)-[e:knows]->(b:person) RETURN e.date;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("PROJECT_DATE_physical"));
}

TEST_F(ExprTest, DATE_PART_DATE32) {
  std::string query =
      "Match (n:person) Return date_part('month', n.birthdate);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("DATE_PART_DATE32_physical"));
}

TEST_F(ExprTest, RETURN_DATE) {
  std::string query = "Return DATE('1995-11-02');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("RETURN_DATE_physical"));
}

TEST_F(ExprTest, RETURN_DATETIME) {
  std::string query = "Return TIMESTAMP('1995-11-02 12:00:00');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("RETURN_DATETIME_physical"));
}

TEST_F(ExprTest, RETURN_INTERVAL) {
  std::string query = "Return INTERVAL('1 year 2 days');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("RETURN_INTERVAL_physical"));
}

TEST_F(ExprTest, DATE_PART_DATE) {
  std::string query = "Return date_part('year', DATE('1995-11-02'));";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("DATE_PART_DATE_physical"));
}

TEST_F(ExprTest, DATE_PART_DATETIME) {
  std::string query =
      "Return date_part('second', TIMESTAMP('1995-11-02 12:05:21'));";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("DATE_PART_DATETIME_physical"));
}

TEST_F(ExprTest, DATE_PART_INTERVAL) {
  std::string query = "Return date_part('year', INTERVAL('1 year 2 days'));";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("DATE_PART_INTERVAL_physical"));
}

TEST_F(ExprTest, DATE_PLUS_INTERVAL) {
  std::string query = "Return DATE('2021-10-12') + INTERVAL('3 DAYS');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("DATE_PLUS_INTERVAL_physical"));
}

TEST_F(ExprTest, DATE_MINUS_INTERVAL) {
  std::string query = "Return DATE('2023-10-11') - INTERVAL('10 DAYS');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("DATE_MINUS_INTERVAL_physical"));
}

TEST_F(ExprTest, TIMESTAMP_MINUS_INTERVAL) {
  std::string query =
      "Return TIMESTAMP('2022-11-12 13:22:17') - INTERVAL('4 minutes 3 hours 2 "
      "days');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("TIMESTAMP_MINUS_INTERVAL_physical"));
}

TEST_F(ExprTest, INTERVAL_PLUS_INTERVAL) {
  std::string query =
      "Return INTERVAL('79 DAYS 32 YEARS') + INTERVAL('20 MILLISECONDS 30 "
      "HOURS 20 DAYS')";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("INTERVAL_PLUS_INTERVAL_physical"));
}

TEST_F(ExprTest, TIMESTAMP_DIFFER) {
  std::string query =
      "Return TIMESTAMP('2022-11-22 15:12:22') - TIMESTAMP('2011-10-09 "
      "13:00:21');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("TIMESTAMP_DIFFER_physical"));
}

TEST_F(ExprTest, DATE_DIFFER) {
  std::string query = "Return date('2011-02-01') - date('2011-01-01');";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("DATE_DIFFER_physical"));
}

TEST_F(ExprTest, LOGICAL_NULL) {
  std::string query = "Match (n) Return n.age > 10 and NULL";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("LOGICAL_NULL_physical"));
}

TEST_F(ExprTest, COMPARE_NULL) {
  std::string query = "Match (n) Return n.age <> NULL";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("COMPARE_NULL_physical"));
}

TEST_F(ExprTest, INT32_MULTI_DOUBLE) {
  std::string query =
      "MATCH (a:person)-[e1:knows]->(b:person) WHERE (a.gender/2 <= 0.5) WITH "
      "b WHERE b.gender*3.5 = 7.0 RETURN COUNT(*)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("INT32_MULTI_DOUBLE_physical"));
}

TEST_F(ExprTest, CAST_TYPE) {
  std::string query =
      "Match (a:person {eyeSight: 1009, gender: 1000}) Return a;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("CAST_TYPE_physical"));
}

TEST_F(ExprTest, CAST_UINT64) {
  std::string query =
      "MATCH (p:person)-[s:studyAt]->(o:organisation) WHERE s.code = 6689 "
      "RETURN p.ID, o.ID";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("CAST_UINT64_physical"));
}

TEST_F(ExprTest, VERTEX_LABEL) {
  std::string query =
      "MATCH (a:person:organisation) RETURN a.ID, label(a), a.fName, a.orgCode";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("VERTEX_LABEL_physical"));
}

TEST_F(ExprTest, VERTEX_LABEL_EQUAL) {
  std::string query =
      "MATCH (a) WHERE label(a)='organisation' RETURN a.orgCode";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("VERTEX_LABEL_EQUAL_physical"));
}

TEST_F(ExprTest, EDGE_LABEL) {
  std::string query =
      "MATCH (a:person)-[b]->(c) RETURN 1 + 1, label(b) LIMIT 2";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("EDGE_LABEL_physical"));
}

TEST_F(ExprTest, COUNT_DISTINCT_LABEL) {
  std::string query = "MATCH (n) return COUNT(DISTINCT label(n));";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("COUNT_DISTINCT_LABEL_physical"));
}

TEST_F(ExprTest, CASE_ELSE) {
  std::string query =
      "MATCH (p: person) RETURN p, CASE WHEN p.registerTime >= "
      "date('2012-01-01') "
      "THEN 1 ELSE 0 END AS valid, CASE WHEN p.registerTime < "
      "date('2011-01-01') "
      "THEN 1 ELSE 0 END";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("CASE_ELSE_physical"));
}

TEST_F(ExprTest, ARRAY_FUNC_LITERAL) {
  std::string query = "Match (n:person) RETURN [3, 2.0, 4, 'name']";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("ARRAY_FUNC_LITERAL_physical"));
}

TEST_F(ExprTest, ARRAY_FUNC_CAST_LITERAL) {
  std::string query =
      "Match (n:person) RETURN [CAST(3, 'uint32'), 2.0, 4, 'name']";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("ARRAY_FUNC_CAST_LITERAL_physical"));
}

TEST_F(ExprTest, ARRAY_FUNC_VAR) {
  std::string query =
      "Match (n:person) RETURN [n.gender, n.fName, n.registerTime]";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("ARRAY_FUNC_VAR_physical"));
}

TEST_F(ExprTest, ORDER_BY_A) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) return a.age, COUNT(b) as c ORDER "
      "BY a.age";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("ORDER_BY_A_physical"));
}

TEST_F(ExprTest, COUNT_AVG_GENDER) {
  std::string query =
      "MATCH (n:person) RETURN n.fName, COUNT(n.gender), AVG(n.gender) ORDER "
      "BY n.fName;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("COUNT_AVG_GENDER_physical"));
}

TEST_F(ExprTest, INTERNAL_ID_FILTER) {
  std::string query = "Match (n:person) Where id(n) = 0 Return n";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("INTERNAL_ID_FILTER_physical"));
}

TEST_F(ExprTest, INTERNAL_ID_FILTER_2) {
  std::string query = "Match (n:person) Where 0=id(n) Return n";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("INTERNAL_ID_FILTER_2_physical"));
}

TEST_F(ExprTest, LIST_EXTRACT) {
  std::string query = R"(
    Match (a:person)
    WITH a ORDER BY a.fName 
    RETURN labels(a) as label, collect(a.fName)[0];
  )";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("LIST_EXTRACT_physical"));
}

TEST_F(ExprTest, CAST_INTERVAL_TO_INT64) {
  std::string query = R"(
    Match (a:person)-[:knows]->(b:person)
    Return CAST(a.registerTime - b.registerTime, 'INT64');
  )";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("CAST_INTERVAL_TO_INT64_physical"));
}

}  // namespace gopt
}  // namespace neug