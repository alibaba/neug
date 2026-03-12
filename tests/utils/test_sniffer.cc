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
namespace neug {
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
  auto schema = sniffer.sniff().value();

  EXPECT_EQ(schema->type(), reader::EntrySchemaType::TABLE);

  auto& columnNames = schema->columnNames;
  EXPECT_EQ(columnNames.size(), 4);
  EXPECT_EQ(columnNames[0], "id");
  EXPECT_EQ(columnNames[1], "name");
  EXPECT_EQ(columnNames[2], "age");
  EXPECT_EQ(columnNames[3], "score");

  auto& columnTypes = schema->columnTypes;
  EXPECT_EQ(columnTypes.size(), 4);
  EXPECT_EQ(columnTypes[0]->primitive_type(),
            ::common::PrimitiveType::DT_SIGNED_INT64);
  EXPECT_EQ(columnTypes[1]->string().item_case(),
            ::common::String::ItemCase::kVarChar);
  EXPECT_EQ(columnTypes[2]->primitive_type(),
            ::common::PrimitiveType::DT_SIGNED_INT64);
  EXPECT_EQ(columnTypes[3]->primitive_type(),
            ::common::PrimitiveType::DT_DOUBLE);
}

}  // namespace test
}  // namespace neug
