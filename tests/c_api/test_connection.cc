/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "test_helpers.h"

TEST_F(CApiTest, SchemaIsAvailableThroughQueries) {
  Contacts();
  Query("CALL SHOW_NODE_TABLES() RETURN vertex_label_name, primary_key;");
  EXPECT_EQ(String(Cell()), "Person");
  EXPECT_EQ(String(Cell(0, 1)), "id");
}

TEST_F(CApiTest, InvalidQueryTextIsRejectedBeforeExecution) {
  for (const auto text : {neug_string_view{}, neug_string_view{nullptr, 1},
                          neug_string_view{"RETURN 1\0", 9}}) {
    EXPECT_EQ(QueryError(text), NEUG_ERROR_INVALID_ARGUMENT);
  }
  Query("RETURN 1;");
  EXPECT_EQ(Integer(Cell()), 1);
}

TEST_F(CApiTest, QueryUsesExplicitLengthAndOwnsItsInput) {
  // Preserve UTF-8 bytes without relying on zero termination or input lifetime.
  const std::string expected = "\xe4\xb8\xad\xe6\x96\x87\xf0\x9f\x99\x82";
  const std::string prefix = "RETURN '" + expected + "';";
  std::string input = prefix + " THIS MUST NOT BE PARSED";
  C_OK(neug_query(connection, {input.data(), prefix.size()}, {}, &result,
                  &error));
  input.assign(input.size(), 'x');
  EXPECT_EQ(String(Cell()), expected);
}

TEST_F(CApiTest, EmptyJsonParametersKeepCompleteQueriesWorking) {
  for (auto parameters : {neug_string_view{}, S(""), S("{}")}) {
    neug_result_destroy(&result);
    C_OK(neug_query(connection, S("RETURN 25;"), parameters, &result, &error));
    EXPECT_EQ(Integer(Cell()), 25);
  }
}

TEST_F(CApiTest, JsonParametersPreserveStringContentsInWrites) {
  Contacts();
  Query("MERGE (p:Person {id:$id}) SET p.name=$name, p.age=$age;",
        "{\"id\":3,\"name\":\"\xe4\xb8\xad\xe6\x96\x87\xf0\x9f\x99\x82"
        R"( O'Connor\\line\nnext\u0000tail","age":42})");
  Query("MATCH (p:Person {id:$id}) RETURN p.name, p.age;", R"({"id":3})");
  EXPECT_EQ(String(Cell()),
            std::string("\xe4\xb8\xad\xe6\x96\x87\xf0\x9f\x99\x82"
                        " O'Connor\\line\nnext") +
                std::string("\0tail", 5));
  EXPECT_EQ(Integer(Cell(0, 1)), 42);
}

TEST_F(CApiTest, JsonParametersPreserveScalarTypesAndIntegerBoundaries) {
  Query(
      "RETURN CAST($signed, 'INT64'), CAST($unsigned, 'UINT64'), "
      "CAST($enabled, 'BOOL'), CAST($ratio, 'DOUBLE'), CAST($missing, "
      "'STRING');",
      R"({"signed":-9223372036854775808,"unsigned":18446744073709551615,"enabled":true,"ratio":1.25,"missing":null})");
  EXPECT_EQ(Integer(Cell()), INT64_MIN);
  auto value = Cell(0, 1);
  uint64_t number = 0;
  C_OK(neug_value_get_uint64(&value, &number, &error));
  EXPECT_EQ(number, UINT64_MAX);
  int64_t signed_number = 0;
  EXPECT_EQ(neug_value_get_int64(&value, &signed_number, &error),
            NEUG_ERROR_OVERFLOW);
  neug_error_destroy(&error);
  value = Cell();
  EXPECT_EQ(neug_value_get_uint64(&value, &number, &error),
            NEUG_ERROR_OVERFLOW);
  neug_error_destroy(&error);
  value = Cell(0, 2);
  uint8_t boolean = 0;
  C_OK(neug_value_get_bool(&value, &boolean, &error));
  EXPECT_EQ(boolean, 1);
  value = Cell(0, 3);
  double ratio = 0;
  C_OK(neug_value_get_double(&value, &ratio, &error));
  EXPECT_DOUBLE_EQ(ratio, 1.25);
  value = Cell(0, 4);
  C_OK(neug_value_is_null(&value, &boolean, &error));
  EXPECT_EQ(boolean, 1);
}

TEST_F(CApiTest, JsonListParametersSupportInPredicatesAndEmptyLists) {
  Contacts();
  Query("MATCH (p:Person) WHERE p.id IN $ids RETURN p.name ORDER BY p.id;",
        R"({"ids":[2,1]})");
  EXPECT_EQ(String(Cell()), "Alice");
  EXPECT_EQ(String(Cell(1)), "Bob");
  Query("MATCH (p:Person) WHERE p.id IN $ids RETURN p.name;", R"({"ids":[]})");
  uint64_t count = 1;
  C_OK(neug_result_row_count(result, &count, &error));
  EXPECT_EQ(count, 0u);
}

TEST_F(CApiTest, JsonParametersUseExplicitLengthAndDoNotBorrowInput) {
  Contacts();
  const std::string prefix = R"({"id":1})";
  std::string input = prefix + " NOT JSON";
  neug_result_destroy(&result);
  C_OK(neug_query(connection, S("MATCH (p:Person {id:$id}) RETURN p.name;"),
                  {input.data(), prefix.size()}, &result, &error));
  input.assign(input.size(), 'x');
  EXPECT_EQ(String(Cell()), "Alice");
}

TEST_F(CApiTest, InvalidJsonParametersAreRejectedBeforeWrites) {
  Contacts();
  const auto query = S("CREATE (:Person {id:3, name:'Unexpected', age:1});");
  const std::string nested =
      "{\"unused\":" + std::string(65, '[') + "0" + std::string(65, ']') + "}";
  const struct {
    neug_string_view json;
    neug_status expected;
  } cases[] = {
      {S("[]"), NEUG_ERROR_INVALID_ARGUMENT},
      {S("null"), NEUG_ERROR_INVALID_ARGUMENT},
      {S("true"), NEUG_ERROR_INVALID_ARGUMENT},
      {S("42"), NEUG_ERROR_INVALID_ARGUMENT},
      {S("\"object\""), NEUG_ERROR_INVALID_ARGUMENT},
      {S("{"), NEUG_ERROR_BAD_ENCODING},
      {S("{} trailing"), NEUG_ERROR_BAD_ENCODING},
      {S(R"({"id":NaN})"), NEUG_ERROR_BAD_ENCODING},
      {S(R"({"id":"\uD800"})"), NEUG_ERROR_BAD_ENCODING},
      {S("{\"id\":\"\xff\"}"), NEUG_ERROR_BAD_ENCODING},
      {{nullptr, 1}, NEUG_ERROR_INVALID_ARGUMENT},
      {{"{}\0trailing", 11}, NEUG_ERROR_INVALID_ARGUMENT},
      {S(nested), NEUG_ERROR_OVERFLOW},
  };
  for (size_t i = 0; i < std::size(cases); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(QueryError(query, cases[i].json), cases[i].expected);
  }
  Query("MATCH (p:Person {id:3}) RETURN count(p);");
  EXPECT_EQ(Integer(Cell()), 0);
}

TEST_F(CApiTest, MissingAndWrongJsonParametersFailWithoutWrites) {
  Contacts();
  for (auto json : {"{}", R"({"age":"wrong"})", R"({"age":1.5})",
                    R"({"age":true})", R"({"age":9223372036854775808})",
                    R"({"age":{}})", R"({"age\u0000suffix":99})"}) {
    SCOPED_TRACE(json);
    QueryError(S("MATCH (p:Person {id:1}) SET p.age=$age;"), S(json));
    Query("MATCH (p:Person {id:1}) RETURN p.age;");
    EXPECT_EQ(Integer(Cell()), 25);
  }
}

TEST_F(CApiTest, JsonParametersWorkInTransactions) {
  Contacts();
  C_OK(neug_connection_begin(connection, 0, &error));
  EXPECT_EQ(QueryError(S("MATCH (p:Person {id:1}) SET p.age=99;"), S("{")),
            NEUG_ERROR_BAD_ENCODING);
  uint8_t active = 0;
  C_OK(neug_connection_has_transaction(connection, &active, &error));
  EXPECT_EQ(active, 1);
  Query("MATCH (p:Person {id:$id}) SET p.age=$age;", R"({"id":1,"age":26})");
  C_OK(neug_connection_rollback(connection, &error));
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 25);
  C_OK(neug_connection_begin(connection, 0, &error));
  Query("MATCH (p:Person {id:$id}) SET p.age=$age;", R"({"id":1,"age":27})");
  C_OK(neug_connection_commit(connection, &error));
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 27);
  C_OK(neug_connection_has_transaction(connection, &active, &error));
  EXPECT_EQ(active, 0);
  QueryError(S("THIS IS NOT CYPHER"));
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 27);
}
