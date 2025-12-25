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

#include "neug/utils/reader/sniffer.h"
#include "test_reader.h"
namespace gs {
namespace test {

class SnifferTest : public ReaderTest {};

TEST_F(SnifferTest, TestSniffBasic) {
  createCsvFile("test_sniff.csv",
                "id|name|age|score\n"
                "1|Alice|25|95.5\n"
                "2|Bob|30|87.0\n"
                "3|Charlie|28|92.5\n");

  auto sharedState = createSharedState("test_sniff.csv", {}, {});

  auto reader = createArrowReader(sharedState);
  auto sniffer = reader::ArrowSniffer(reader);
  auto schema = sniffer.sniff();

  EXPECT_EQ(schema->type(), reader::EntrySchemaType::TABLE);

  auto& columnNames = schema->columnNames;
  EXPECT_EQ(columnNames.size(), 4);
  EXPECT_EQ(columnNames[0], "id");
  EXPECT_EQ(columnNames[1], "name");
  EXPECT_EQ(columnNames[2], "age");
  EXPECT_EQ(columnNames[3], "score");

  auto& columnTypes = schema->columnTypes;
  EXPECT_EQ(columnTypes[0]->DebugString(), createInt64Type()->DebugString());
  EXPECT_EQ(columnTypes[1]->DebugString(), createStringType()->DebugString());
  EXPECT_EQ(columnTypes[2]->DebugString(), createInt64Type()->DebugString());
  EXPECT_EQ(columnTypes[3]->DebugString(), createDoubleType()->DebugString());
}

TEST_F(SnifferTest, TestAdaptiveSniffVertexDefault) {
  createCsvFile("test_vertex.csv",
                "id|name|age\n"
                "1|Alice|25\n"
                "2|Bob|30\n");

  auto sharedState = createSharedState("test_vertex.csv", {}, {});
  auto reader = createArrowReader(sharedState);
  auto sniffer = reader::ArrowSniffer(reader);

  reader::VertexEntrySchema vertexOptions;
  vertexOptions.label = "Person";

  auto result = sniffer.adaptiveSniff(vertexOptions);
  EXPECT_EQ(result->type(), reader::EntrySchemaType::VERTEX);
  auto vertexSchema = result->ptrCast<reader::VertexEntrySchema>();
  ASSERT_NE(vertexSchema, nullptr);

  EXPECT_EQ("Person", vertexSchema->label);
  EXPECT_EQ(vertexSchema->primaryCol, "id");
  EXPECT_EQ(vertexSchema->columnNames.size(), 3);
  EXPECT_EQ(vertexSchema->columnTypes.size(), 3);
}

TEST_F(SnifferTest, TestAdaptiveSniffVertexCustom) {
  createCsvFile("test_vertex_custom.csv",
                "id|name|age\n"
                "1|Alice|25\n"
                "2|Bob|30\n");

  auto sharedState = createSharedState("test_vertex_custom.csv", {}, {});
  auto reader = createArrowReader(sharedState);
  auto sniffer = reader::ArrowSniffer(reader);

  reader::VertexEntrySchema vertexOptions;
  vertexOptions.label = "Person";
  vertexOptions.primaryCol = "name";

  auto result = sniffer.adaptiveSniff(vertexOptions);
  EXPECT_EQ(result->type(), reader::EntrySchemaType::VERTEX);

  auto vertexSchema = result->ptrCast<reader::VertexEntrySchema>();
  ASSERT_NE(vertexSchema, nullptr);

  EXPECT_EQ(vertexSchema->label, "Person");
  EXPECT_EQ(vertexSchema->primaryCol, "name");
}

TEST_F(SnifferTest, TestAdaptiveSniffEdgeDefault) {
  createCsvFile("test_edge.csv",
                "src_id|dst_id|weight|label\n"
                "1|2|0.5|friend\n"
                "2|3|0.8|colleague\n");

  auto sharedState = createSharedState("test_edge.csv", {}, {});
  auto reader = createArrowReader(sharedState);
  auto sniffer = reader::ArrowSniffer(reader);

  reader::EdgeEntrySchema edgeOptions;
  edgeOptions.label = "KNOWS";
  edgeOptions.srcLabel = "Person";
  edgeOptions.dstLabel = "Person";

  auto result = sniffer.adaptiveSniff(edgeOptions);
  EXPECT_EQ(result->type(), reader::EntrySchemaType::EDGE);

  auto edgeSchema = result->ptrCast<reader::EdgeEntrySchema>();
  ASSERT_NE(edgeSchema, nullptr);

  EXPECT_EQ(edgeSchema->label, "KNOWS");
  EXPECT_EQ(edgeSchema->srcLabel, "Person");
  EXPECT_EQ(edgeSchema->dstLabel, "Person");
  EXPECT_EQ(edgeSchema->srcCol, "src_id");
  EXPECT_EQ(edgeSchema->dstCol, "dst_id");
  EXPECT_EQ(edgeSchema->columnNames.size(), 4);
}

TEST_F(SnifferTest, TestAdaptiveSniffEdgeCustom) {
  createCsvFile("test_edge_custom.csv",
                "dst_id|src_id|weight|type\n"
                "1|2|0.5|friend\n"
                "2|3|0.8|colleague\n");

  auto sharedState = createSharedState("test_edge_custom.csv", {}, {});
  auto reader = createArrowReader(sharedState);
  auto sniffer = reader::ArrowSniffer(reader);

  reader::EdgeEntrySchema edgeOptions;
  edgeOptions.label = "KNOWS";
  edgeOptions.srcLabel = "Person";
  edgeOptions.dstLabel = "Person";
  edgeOptions.srcCol = "src_id";
  edgeOptions.dstCol = "dst_id";

  auto result = sniffer.adaptiveSniff(edgeOptions);
  EXPECT_EQ(result->type(), reader::EntrySchemaType::EDGE);

  auto edgeSchema = result->ptrCast<reader::EdgeEntrySchema>();
  ASSERT_NE(edgeSchema, nullptr);

  EXPECT_EQ(edgeSchema->label, "KNOWS");
  EXPECT_EQ(edgeSchema->srcLabel, "Person");
  EXPECT_EQ(edgeSchema->dstLabel, "Person");
  EXPECT_EQ(edgeSchema->srcCol,
            "src_id");  // second column name in original csv file
  EXPECT_EQ(edgeSchema->dstCol,
            "dst_id");  // first column name in original csv file
}

TEST_F(SnifferTest, TestAdaptiveSniffVertexNoHeader) {
  createCsvFile("test_vertex_no_header.csv",
                "1|Alice|25\n"
                "2|Bob|30\n"
                "3|Charlie|28\n");

  auto sharedState = createSharedState("test_vertex_no_header.csv", {}, {},
                                       {{"autogenerate_column_names", "true"}});
  auto reader = createArrowReader(sharedState);
  auto sniffer = reader::ArrowSniffer(reader);

  reader::VertexEntrySchema vertexOptions;
  vertexOptions.label = "Person";

  auto result = sniffer.adaptiveSniff(vertexOptions);
  EXPECT_EQ(result->type(), reader::EntrySchemaType::VERTEX);

  auto vertexSchema = result->ptrCast<reader::VertexEntrySchema>();
  ASSERT_NE(vertexSchema, nullptr);

  EXPECT_EQ("Person", vertexSchema->label);
  EXPECT_EQ(vertexSchema->primaryCol, "f0");
  EXPECT_EQ(vertexSchema->columnNames.size(), 3);
  // arrow will auto-generate column names like f0, f1, f2 if header is not
  // provided
  EXPECT_EQ(vertexSchema->columnNames[0], "f0");
  EXPECT_EQ(vertexSchema->columnNames[1], "f1");
  EXPECT_EQ(vertexSchema->columnNames[2], "f2");
  EXPECT_EQ(vertexSchema->columnTypes.size(), 3);
}

}  // namespace test
}  // namespace gs
