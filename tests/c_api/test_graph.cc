/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "test_helpers.h"

TEST_F(CApiTest, NodesRelationshipsAndPathsUseGenericReaders) {
  Contacts();
  Query("MATCH p=(a:Person {id:1})-[r:KNOWS]->(b:Person) RETURN a,r,p;");
  auto node = Cell();
  auto relationship = Cell(0, 1);
  auto graph_path = Cell(0, 2);
  neug_type type = NEUG_TYPE_UNKNOWN;
  C_OK(neug_value_type(&node, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_NODE);
  C_OK(neug_value_type(&relationship, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_RELATIONSHIP);
  C_OK(neug_value_type(&graph_path, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_PATH);
  EXPECT_EQ(String(Field(node, "_LABEL")), "Person");
  const auto id = Integer(Field(node, "_ID"));
  const auto source = Integer(Field(relationship, "_SRC_ID"));
  const auto destination = Integer(Field(relationship, "_DST_ID"));
  EXPECT_EQ(id, source);
  EXPECT_NE(source, destination);
  auto saved = Field(node, "name");
  EXPECT_EQ(String(saved), "Alice");
  auto nodes = Field(graph_path, "nodes");
  auto edges = Field(graph_path, "rels");
  uint64_t count = 0;
  C_OK(neug_value_size(&nodes, &count, &error));
  EXPECT_EQ(count, 2u);
  C_OK(neug_value_size(&edges, &count, &error));
  EXPECT_EQ(count, 1u);
  EXPECT_EQ(Integer(Field(graph_path, "length")), 1);
  auto child = Child(nodes, 1);
  C_OK(neug_value_type(&child, &type, &error));
  EXPECT_EQ(type, NEUG_TYPE_OBJECT);
  EXPECT_EQ(String(Field(child, "name")), "Bob");
  EXPECT_EQ(Integer(Field(child, "_ID")), destination);
  child = Child(edges, 0);
  EXPECT_EQ(String(Field(child, "_LABEL")), "KNOWS");
  EXPECT_EQ(Integer(Field(child, "since")), 2024);
  EXPECT_EQ(Integer(Field(child, "_ID")), Integer(Field(relationship, "_ID")));
  C_OK(neug_connection_destroy(&connection, &error));
  C_OK(neug_db_close(db, &error));
  C_OK(neug_db_destroy(&db, &error));
  EXPECT_EQ(String(saved), "Alice");
  EXPECT_EQ(String(Field(Child(nodes, 1), "name")), "Bob");
}
