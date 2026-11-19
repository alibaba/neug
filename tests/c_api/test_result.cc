/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "test_helpers.h"

#include <cmath>

TEST_F(CApiTest, TemporalAndIntervalValues) {
  Query(
      "RETURN date('1970-01-02'), timestamp('1970-01-01 00:00:01'), "
      "INTERVAL('1 day');");
  EXPECT_EQ(Integer(Cell()), 86400000);
  EXPECT_EQ(Integer(Cell(0, 1)), 1000);
  EXPECT_FALSE(String(Cell(0, 2)).empty());
}

TEST_F(CApiTest, NonFiniteResultsRemainReadable) {
  Query("RETURN CAST('NaN', 'DOUBLE'), CAST('Infinity', 'DOUBLE');");
  auto value = Cell();
  double number = 0;
  C_OK(neug_value_get_double(&value, &number, &error));
  EXPECT_TRUE(std::isnan(number));
  value = Cell(0, 1);
  C_OK(neug_value_get_double(&value, &number, &error));
  EXPECT_TRUE(std::isinf(number));
  EXPECT_GT(number, 0);
}

TEST_F(CApiTest, RandomAccessMetadataAndBounds) {
  Contacts();
  Query("MATCH (p:Person) RETURN p.id AS id, p.name AS name ORDER BY p.id;");
  uint64_t count = 0;
  C_OK(neug_result_row_count(result, &count, &error));
  EXPECT_EQ(count, 2u);
  C_OK(neug_result_column_count(result, &count, &error));
  EXPECT_EQ(count, 2u);
  neug_string_view name{};
  C_OK(neug_result_column_name(result, 1, &name, &error));
  EXPECT_EQ(Str(name), "name");
  EXPECT_EQ(String(Cell(1, 1)), "Bob");
  EXPECT_EQ(String(Cell(0, 1)), "Alice");
  neug_value_view value{};
  EXPECT_EQ(neug_result_get_value(result, 2, 0, &value, &error),
            NEUG_ERROR_INDEX);
  EXPECT_EQ(value._owner, nullptr);
  neug_error_destroy(&error);
  EXPECT_EQ(neug_result_get_value(result, 0, 2, &value, &error),
            NEUG_ERROR_INDEX);
  neug_error_destroy(&error);
  EXPECT_EQ(neug_result_get_value(result, UINT64_MAX, 0, &value, &error),
            NEUG_ERROR_INDEX);
  neug_error_destroy(&error);
  EXPECT_EQ(neug_result_column_name(result, 2, &name, &error),
            NEUG_ERROR_INDEX);
  EXPECT_EQ(name.data, nullptr);
  neug_error_destroy(&error);
  value = Cell(0, 1);
  int64_t number = 99;
  EXPECT_EQ(neug_value_get_int64(&value, &number, &error), NEUG_ERROR_TYPE);
  EXPECT_EQ(number, 0);
  neug_error_destroy(&error);
}

TEST_F(CApiTest, ListsTuplesAndNamedFields) {
  Query("RETURN CAST([1,CAST(NULL,'INT64'),3], 'INT64[]'), [1,'a'];");
  auto list = Cell();
  auto item = Child(list, 1);
  uint8_t is_null = 0;
  C_OK(neug_value_is_null(&item, &is_null, &error));
  EXPECT_EQ(is_null, 1);
  int64_t number = 0;
  EXPECT_EQ(neug_value_get_int64(&item, &number, &error), NEUG_ERROR_TYPE);
  neug_error_destroy(&error);
  EXPECT_EQ(neug_value_child(&list, 3, &item, &error), NEUG_ERROR_INDEX);
  neug_error_destroy(&error);
  auto tuple = Cell(0, 1);
  neug_type type = NEUG_TYPE_UNKNOWN;
  C_OK(neug_value_type(&tuple, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_TUPLE);
  EXPECT_EQ(Integer(Child(tuple, 0)), 1);
  EXPECT_EQ(String(Child(tuple, 1)), "a");

  Query(
      "RETURN CAST({name:'Alice', age:25}, 'STRUCT(name STRING, age INT64)');");
  auto structure = Cell();
  neug_string_view name{};
  C_OK(neug_value_field_name(&structure, 0, &name, &error));
  EXPECT_EQ(Str(name), "name");
  EXPECT_EQ(String(Child(structure, 0)), "Alice");
  C_OK(neug_value_field_name(&structure, 1, &name, &error));
  EXPECT_EQ(Str(name), "age");
  EXPECT_EQ(Integer(Child(structure, 1)), 25);
  EXPECT_EQ(neug_value_field_name(&structure, 2, &name, &error),
            NEUG_ERROR_INDEX);
  neug_error_destroy(&error);
}

TEST_F(CApiTest, EmptyResultRetainsColumnMetadata) {
  Contacts();
  Query("MATCH (p:Person) WHERE p.id=999 RETURN p.name AS name;");
  uint64_t count = 1;
  C_OK(neug_result_row_count(result, &count, &error));
  EXPECT_EQ(count, 0u);
  C_OK(neug_result_column_count(result, &count, &error));
  EXPECT_EQ(count, 1u);
  neug_string_view name{};
  C_OK(neug_result_column_name(result, 0, &name, &error));
  EXPECT_EQ(Str(name), "name");
  neug_type type = NEUG_TYPE_UNKNOWN;
  C_OK(neug_result_column_type(result, 0, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_STRING);
  neug_value_view value{};
  EXPECT_EQ(neug_result_get_value(result, 0, 0, &value, &error),
            NEUG_ERROR_INDEX);
  neug_error_destroy(&error);
}

TEST_F(CApiTest, ViewsCanBeReusedForNestedChildReads) {
  Query(
      "RETURN CAST({ages:CAST([25,26], 'INT64[]')}, 'STRUCT(ages INT64[])');");
  auto value = Cell();
  neug_type type = NEUG_TYPE_UNKNOWN;
  C_OK(neug_value_type(&value, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_STRUCT);
  EXPECT_EQ(Integer(Child(Field(value, "ages"), 1)), 26);
  C_OK(neug_value_child(&value, 0, &value, &error));
  C_OK(neug_value_child(&value, 1, &value, &error));
  EXPECT_EQ(Integer(value), 26);

  value = Cell();
  EXPECT_EQ(neug_value_child(&value, 1, &value, &error), NEUG_ERROR_INDEX);
  EXPECT_EQ(value._owner, nullptr);
  neug_error_destroy(&error);
  value = Cell();
  C_OK(neug_value_child(&value, 0, &value, &error));
  EXPECT_EQ(neug_value_child(&value, 2, &value, &error), NEUG_ERROR_INDEX);
  EXPECT_EQ(value._owner, nullptr);
  neug_error_destroy(&error);

  value = Cell();
  EXPECT_EQ(neug_value_child(nullptr, 0, &value, &error),
            NEUG_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(value._owner, nullptr);
  neug_error_destroy(&error);
  neug_string_view name = S("stale");
  EXPECT_EQ(neug_value_field_name(nullptr, 0, &name, &error),
            NEUG_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(name.data, nullptr);
  EXPECT_EQ(name.size, 0u);
  neug_error_destroy(&error);
}
