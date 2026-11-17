/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "test_helpers.h"

TEST_F(CApiTest, TransactionsAndQueryErrors) {
  Contacts();
  C_OK(neug_connection_begin(connection, 0, &error));
  uint8_t active = 0;
  C_OK(neug_connection_has_transaction(connection, &active, &error));
  EXPECT_EQ(active, 1);
  Query("MATCH (p:Person {id:1}) SET p.age=26;");
  C_OK(neug_connection_rollback(connection, &error));
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 25);
  C_OK(neug_connection_begin(connection, 0, &error));
  Query("MATCH (p:Person {id:1}) SET p.age=27;");
  C_OK(neug_connection_commit(connection, &error));
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 27);
  C_OK(neug_connection_has_transaction(connection, &active, &error));
  EXPECT_EQ(active, 0);
  neug_result* failed = nullptr;
  EXPECT_NE(
      neug_query(connection, S("THIS IS NOT CYPHER"), {}, &failed, &error),
      NEUG_OK);
  EXPECT_EQ(failed, nullptr);
  neug_error_destroy(&error);
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 27);
}

TEST_F(CApiTest, SchemaIsAvailableThroughQueries) {
  Contacts();
  Query("CALL SHOW_NODE_TABLES() RETURN vertex_label_name, primary_key;");
  EXPECT_EQ(String(Cell()), "Person");
  EXPECT_EQ(String(Cell(0, 1)), "id");
  Query(
      "CALL SHOW_NODE_TABLE_INFO('Person') RETURN property_name, "
      "property_type, primary_key;");
  uint64_t count = 0;
  C_OK(neug_result_row_count(result, &count, &error));
  EXPECT_EQ(count, 3u);
  EXPECT_EQ(String(Cell()), "id");
  EXPECT_EQ(String(Cell(0, 1)), "INT64");
  auto key = Cell(0, 2);
  uint8_t primary = 0;
  C_OK(neug_value_get_bool(&key, &primary, &error));
  EXPECT_EQ(primary, 1);
  Query(
      "CALL SHOW_REL_TABLE_INFO('[Person, KNOWS, Person]') "
      "RETURN property_name, property_type;");
  EXPECT_EQ(String(Cell()), "since");
  EXPECT_EQ(String(Cell(0, 1)), "INT64");
}

TEST_F(CApiTest, InvalidQueryTextIsRejectedBeforeExecution) {
  for (const auto text : {neug_string_view{}, neug_string_view{nullptr, 1},
                          neug_string_view{"RETURN 1\0", 9}}) {
    neug_result* failed = nullptr;
    EXPECT_EQ(neug_query(connection, text, {}, &failed, &error),
              NEUG_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(failed, nullptr);
    neug_error_destroy(&error);
  }
  Query("RETURN 1;");
  EXPECT_EQ(Integer(Cell()), 1);
}

TEST_F(CApiTest, QueryUsesExplicitLengthAndOwnsItsInput) {
  const std::string prefix = "RETURN 25;";
  std::string input = prefix + " THIS MUST NOT BE PARSED";
  C_OK(neug_query(connection, {input.data(), prefix.size()}, {}, &result,
                  &error));
  input.assign(input.size(), 'x');
  EXPECT_EQ(Integer(Cell()), 25);
}

TEST_F(CApiTest, CompleteQueriesPreserveUnicodeAndEscapedText) {
  // UTF-8 bytes for U+4E2D, U+6587, and U+1F642, independent of source
  // encoding.
  Query(
      "RETURN '\xe4\xb8\xad\xe6\x96\x87\xf0\x9f\x99\x82', "
      R"('O\'Connor', 'C:\\data';)");
  EXPECT_EQ(String(Cell()), "\xe4\xb8\xad\xe6\x96\x87\xf0\x9f\x99\x82");
  EXPECT_EQ(String(Cell(0, 1)), "O'Connor");
  EXPECT_EQ(String(Cell(0, 2)), R"(C:\data)");
  Query("RETURN 'line\nnext';");
  EXPECT_EQ(String(Cell()), "line\nnext");
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
  for (auto json : {"[]", "null", "true", "42", "\"object\""}) {
    neug_result* failed = nullptr;
    EXPECT_EQ(neug_query(connection, query, S(json), &failed, &error),
              NEUG_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(failed, nullptr);
    neug_error_destroy(&error);
  }
  for (auto json : {"{", "{} trailing", R"({"id":NaN})", R"({"id":"\uD800"})",
                    "{\"id\":\"\xff\"}"}) {
    neug_result* failed = nullptr;
    EXPECT_EQ(neug_query(connection, query, S(json), &failed, &error),
              NEUG_ERROR_BAD_ENCODING);
    EXPECT_EQ(failed, nullptr);
    ASSERT_NE(error, nullptr);
    EXPECT_GT(neug_error_message(error).size, 0u);
    neug_error_destroy(&error);
  }
  for (auto json :
       {neug_string_view{nullptr, 1}, neug_string_view{"{}\0trailing", 11}}) {
    neug_result* failed = nullptr;
    EXPECT_EQ(neug_query(connection, query, json, &failed, &error),
              NEUG_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(failed, nullptr);
    neug_error_destroy(&error);
  }
  const std::string nested =
      "{\"unused\":" + std::string(65, '[') + "0" + std::string(65, ']') + "}";
  neug_result* failed = nullptr;
  EXPECT_EQ(neug_query(connection, query, S(nested), &failed, &error),
            NEUG_ERROR_OVERFLOW);
  EXPECT_EQ(failed, nullptr);
  neug_error_destroy(&error);
  Query("MATCH (p:Person {id:3}) RETURN count(p);");
  EXPECT_EQ(Integer(Cell()), 0);
}

TEST_F(CApiTest, MissingAndWrongJsonParametersFailWithoutWrites) {
  Contacts();
  for (auto json : {"{}", R"({"age":"wrong"})", R"({"age":1.5})",
                    R"({"age":true})", R"({"age":9223372036854775808})",
                    R"({"age":{}})", R"({"age\u0000suffix":99})"}) {
    SCOPED_TRACE(json);
    neug_result* failed = nullptr;
    EXPECT_NE(
        neug_query(connection, S("MATCH (p:Person {id:1}) SET p.age=$age;"),
                   S(json), &failed, &error),
        NEUG_OK);
    EXPECT_EQ(failed, nullptr);
    neug_error_destroy(&error);
    Query("MATCH (p:Person {id:1}) RETURN p.age;");
    EXPECT_EQ(Integer(Cell()), 25);
  }
}

TEST_F(CApiTest, JsonParametersWorkInTransactions) {
  Contacts();
  C_OK(neug_connection_begin(connection, 0, &error));
  neug_result* failed = nullptr;
  EXPECT_EQ(neug_query(connection, S("MATCH (p:Person {id:1}) SET p.age=99;"),
                       S("{"), &failed, &error),
            NEUG_ERROR_BAD_ENCODING);
  EXPECT_EQ(failed, nullptr);
  neug_error_destroy(&error);
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
}

TEST_F(CApiTest, ConnectionCloseRollsBackAndResultSurvives) {
  Contacts();
  Query("MATCH (p:Person {id:1}) RETURN p.name;");
  auto value = Cell();
  C_OK(neug_connection_begin(connection, 0, &error));
  neug_result* write = nullptr;
  C_OK(neug_query(connection, S("MATCH (p:Person {id:1}) SET p.age=99;"), {},
                  &write, &error));
  neug_result_destroy(&write);
  C_OK(neug_connection_destroy(&connection, &error));
  EXPECT_EQ(String(value), "Alice");
  C_OK(neug_db_connect(db, &connection, &error));
  Query("MATCH (p:Person {id:1}) RETURN p.age;");
  EXPECT_EQ(Integer(Cell()), 25);
}
