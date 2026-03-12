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

#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"

namespace neug {
namespace execution {
namespace test {
class VertexColumnTest : public ::testing::Test {
 protected:
  static constexpr label_t kLabel0 = 0;
  static constexpr label_t kLabel1 = 1;
  static constexpr vid_t kVid0 = 100;
  static constexpr vid_t kVid1 = 101;
  static constexpr vid_t kVid2 = 102;
  static constexpr vid_t kNullVid = std::numeric_limits<vid_t>::max();
  static constexpr label_t kNullLabel = std::numeric_limits<label_t>::max();

  std::shared_ptr<SLVertexColumn> build_sl_vertex_column(label_t label,
                                                         bool is_optional) {
    MSVertexColumnBuilder col_builder(label);
    col_builder.push_back_elem(Value::VERTEX(VertexRecord(label, kVid0)));
    col_builder.push_back_vertex(VertexRecord(label, kVid1));
    if (is_optional) {
      col_builder.push_back_null();
    }
    std::shared_ptr<IContextColumn> col = col_builder.finish();
    return std::dynamic_pointer_cast<SLVertexColumn>(col);
  }

  std::shared_ptr<MSVertexColumn> build_ms_vertex_column(bool is_optional) {
    MSVertexColumnBuilder col_builder(kLabel0);
    col_builder.push_back_vertex(VertexRecord(kLabel0, kVid0));
    col_builder.push_back_vertex(VertexRecord(kLabel0, kVid1));
    col_builder.push_back_vertex(VertexRecord(kLabel1, kVid1));
    col_builder.push_back_vertex(VertexRecord(kLabel1, kVid2));
    if (is_optional) {
      col_builder.push_back_null();
    }
    std::shared_ptr<IContextColumn> col = col_builder.finish();
    return std::dynamic_pointer_cast<MSVertexColumn>(col);
  }

  std::shared_ptr<MLVertexColumn> build_ml_vertex_column(bool is_optional) {
    MLVertexColumnBuilder col_builder;
    col_builder.push_back_vertex(VertexRecord(kLabel0, kVid0));
    col_builder.push_back_vertex(VertexRecord(kLabel0, kVid1));
    col_builder.push_back_vertex(VertexRecord(kLabel1, kVid1));
    col_builder.push_back_vertex(VertexRecord(kLabel1, kVid2));
    if (is_optional) {
      col_builder.push_back_null();
    }
    std::shared_ptr<IContextColumn> col = col_builder.finish();
    return std::dynamic_pointer_cast<MLVertexColumn>(col);
  }
};

TEST_F(VertexColumnTest, SLVertexColumnBasic) {
  std::shared_ptr<SLVertexColumn> sl_col =
      this->build_sl_vertex_column(kLabel0, false);

  EXPECT_EQ(sl_col->size(), 2);
  EXPECT_EQ(sl_col->column_type(), ContextColumnType::kVertex);
  EXPECT_EQ(sl_col->column_info(), "SLVertexColumn(0)[2]");
  EXPECT_EQ(sl_col->elem_type().id(), DataTypeId::kVertex);

  EXPECT_EQ(sl_col->vertex_column_type(), VertexColumnType::kSingle);
  EXPECT_EQ(sl_col->label(), kLabel0);
  EXPECT_EQ(sl_col->get_vertex(0), VertexRecord(kLabel0, kVid0));
  EXPECT_EQ(sl_col->get_vertex(1), VertexRecord(kLabel0, kVid1));

  std::set<label_t> labels = sl_col->get_labels_set();
  EXPECT_EQ(labels.size(), 1);
  EXPECT_EQ(*labels.begin(), kLabel0);
}

TEST_F(VertexColumnTest, SLVertexColumnOptional) {
  std::shared_ptr<SLVertexColumn> sl_optional_col =
      this->build_sl_vertex_column(kLabel0, true);

  EXPECT_EQ(sl_optional_col->size(), 3);
  EXPECT_EQ(sl_optional_col->column_type(), ContextColumnType::kVertex);

  EXPECT_EQ(sl_optional_col->vertex_column_type(), VertexColumnType::kSingle);
  EXPECT_TRUE(sl_optional_col->is_optional());
  EXPECT_EQ(sl_optional_col->label(), kLabel0);
  EXPECT_EQ(sl_optional_col->get_vertex(0), VertexRecord(kLabel0, kVid0));
  EXPECT_EQ(sl_optional_col->get_vertex(1), VertexRecord(kLabel0, kVid1));
  EXPECT_TRUE(sl_optional_col->has_value(0));
  EXPECT_TRUE(sl_optional_col->has_value(1));
  EXPECT_FALSE(sl_optional_col->has_value(2));
}

TEST_F(VertexColumnTest, SLVertexColumnForeach) {
  std::shared_ptr<SLVertexColumn> sl_col =
      this->build_sl_vertex_column(kLabel0, false);

  std::vector<VertexRecord> collected;
  sl_col->foreach_vertex([&](size_t idx, label_t label, vid_t vid) {
    collected.push_back({label, vid});
  });

  ASSERT_EQ(collected.size(), 2);
  EXPECT_EQ(collected[0], (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(collected[1], (VertexRecord{kLabel0, kVid1}));
}

TEST_F(VertexColumnTest, SLVertexColumnShuffle) {
  std::shared_ptr<SLVertexColumn> sl_col =
      this->build_sl_vertex_column(kLabel0, false);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled = sl_col->shuffle(offsets);
  auto* ms_col = dynamic_cast<SLVertexColumn*>(shuffled.get());
  ASSERT_NE(ms_col, nullptr);
  EXPECT_EQ(ms_col->get_vertex(0), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(ms_col->get_vertex(1), (VertexRecord{kLabel0, kVid0}));
}

TEST_F(VertexColumnTest, SLVertexColumnOptionalShuffle) {
  std::shared_ptr<SLVertexColumn> sl_optional_col =
      this->build_sl_vertex_column(kLabel0, true);

  std::vector<size_t> offsets = {std::numeric_limits<size_t>::max(), 1, 2};
  auto shuffled = sl_optional_col->optional_shuffle(offsets);
  ASSERT_EQ(shuffled->size(), 3);

  auto* ms_col = dynamic_cast<SLVertexColumn*>(shuffled.get());
  ASSERT_NE(ms_col, nullptr);
  EXPECT_FALSE(ms_col->has_value(0));
  EXPECT_TRUE(ms_col->has_value(1));
  EXPECT_FALSE(ms_col->has_value(2));
}

TEST_F(VertexColumnTest, SLVertexColumnUnionSameLabel) {
  std::shared_ptr<SLVertexColumn> sl_col_1 =
      this->build_sl_vertex_column(kLabel0, false);
  std::shared_ptr<SLVertexColumn> sl_col_2 =
      this->build_sl_vertex_column(kLabel0, false);

  auto unioned = sl_col_1->union_col(sl_col_2);
  ASSERT_EQ(unioned->size(), 4);

  auto* sl_col = dynamic_cast<SLVertexColumn*>(unioned.get());
  ASSERT_NE(sl_col, nullptr);
  EXPECT_EQ(sl_col->get_vertex(0), (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(sl_col->get_vertex(1), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(sl_col->get_vertex(2), (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(sl_col->get_vertex(3), (VertexRecord{kLabel0, kVid1}));
}

TEST_F(VertexColumnTest, SLVertexColumnUnionDiffLabel) {
  std::shared_ptr<SLVertexColumn> sl_col_1 =
      this->build_sl_vertex_column(kLabel0, false);
  std::shared_ptr<SLVertexColumn> sl_col_2 =
      this->build_sl_vertex_column(kLabel1, false);

  auto unioned = sl_col_1->union_col(sl_col_2);
  ASSERT_EQ(unioned->size(), 4);

  auto* ml_col = dynamic_cast<MLVertexColumn*>(unioned.get());
  ASSERT_NE(ml_col, nullptr);
  EXPECT_EQ(ml_col->get_vertex(0), (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(ml_col->get_vertex(1), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(ml_col->get_vertex(2), (VertexRecord{kLabel1, kVid0}));
  EXPECT_EQ(ml_col->get_vertex(3), (VertexRecord{kLabel1, kVid1}));
}

TEST_F(VertexColumnTest, SLVertexColumnDedup) {
  std::shared_ptr<SLVertexColumn> sl_col_1 =
      this->build_sl_vertex_column(kLabel0, false);
  std::shared_ptr<SLVertexColumn> sl_col_2 =
      this->build_sl_vertex_column(kLabel0, true);

  auto unioned = sl_col_1->union_col(sl_col_2);
  ASSERT_EQ(unioned->size(), 5);

  auto* sl_col = dynamic_cast<SLVertexColumn*>(unioned.get());
  std::vector<size_t> offsets;
  sl_col->generate_dedup_offset(offsets);

  std::set<vid_t> expected_vids = {kVid0, kVid1, kNullVid};
  std::set<vid_t> actual_vids;
  for (auto idx : offsets) {
    actual_vids.insert(sl_col->get_vertex(idx).vid());
  }
  EXPECT_EQ(actual_vids, expected_vids);
}

TEST_F(VertexColumnTest, SLVertexColumnAggregate) {
  std::shared_ptr<SLVertexColumn> sl_col_1 =
      this->build_sl_vertex_column(kLabel0, false);
  std::shared_ptr<SLVertexColumn> sl_col_2 =
      this->build_sl_vertex_column(kLabel0, false);

  auto unioned = sl_col_1->union_col(sl_col_2);
  ASSERT_EQ(unioned->size(), 4);

  auto* sl_col = dynamic_cast<SLVertexColumn*>(unioned.get());
  auto [i_col, offsets] = sl_col->generate_aggregate_offset();
  std::shared_ptr<SLVertexColumn> aggregated_col =
      std::dynamic_pointer_cast<SLVertexColumn>(i_col);

  std::set<vid_t> expected_vids = {kVid0, kVid1};
  std::set<vid_t> actual_vids;
  for (auto idx = 0; idx < offsets.size(); idx++) {
    actual_vids.insert(aggregated_col->get_vertex(idx).vid());
  }
  EXPECT_EQ(actual_vids, expected_vids);
}

TEST_F(VertexColumnTest, MSVertexColumnBasic) {
  std::shared_ptr<MSVertexColumn> ms_col = this->build_ms_vertex_column(false);

  EXPECT_EQ(ms_col->size(), 4);
  EXPECT_EQ(ms_col->vertex_column_type(), VertexColumnType::kMultiSegment);
  EXPECT_EQ(ms_col->column_info(), "MSVertexColumn(0, 1)[4]");
  EXPECT_EQ(ms_col->elem_type().id(), DataTypeId::kVertex);

  EXPECT_EQ(ms_col->get_vertex(0), (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(ms_col->get_vertex(1), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(ms_col->get_vertex(2), (VertexRecord{kLabel1, kVid1}));
  EXPECT_EQ(ms_col->get_vertex(3), (VertexRecord{kLabel1, kVid2}));
}

TEST_F(VertexColumnTest, MSVertexColumnForeach) {
  std::shared_ptr<MSVertexColumn> ms_col = this->build_ms_vertex_column(false);

  std::vector<VertexRecord> collected;
  ms_col->foreach_vertex([&](size_t idx, label_t label, vid_t vid) {
    collected.push_back({label, vid});
  });

  ASSERT_EQ(collected.size(), 4);
  EXPECT_EQ(collected[0], (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(collected[1], (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(collected[2], (VertexRecord{kLabel1, kVid1}));
  EXPECT_EQ(collected[3], (VertexRecord{kLabel1, kVid2}));
}

TEST_F(VertexColumnTest, MSVertexColumnShuffle) {
  std::shared_ptr<MSVertexColumn> ms_col = this->build_ms_vertex_column(true);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled = ms_col->shuffle(offsets);
  auto* shuffled_col = dynamic_cast<MLVertexColumn*>(shuffled.get());
  ASSERT_NE(shuffled_col, nullptr);
  EXPECT_EQ(shuffled_col->get_vertex(0), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(shuffled_col->get_vertex(1), (VertexRecord{kLabel0, kVid0}));
}

TEST_F(VertexColumnTest, MSVertexColumnOptionalShuffle) {
  std::shared_ptr<MSVertexColumn> ms_optional_col =
      this->build_ms_vertex_column(true);

  std::vector<size_t> offsets = {std::numeric_limits<size_t>::max(), 1, 4};
  auto shuffled = ms_optional_col->optional_shuffle(offsets);
  ASSERT_EQ(shuffled->size(), 3);

  auto* shuffled_col = dynamic_cast<MLVertexColumn*>(shuffled.get());
  ASSERT_NE(shuffled_col, nullptr);
  EXPECT_FALSE(shuffled_col->has_value(0));
  EXPECT_TRUE(shuffled_col->has_value(1));
  EXPECT_FALSE(shuffled_col->has_value(2));
}

TEST_F(VertexColumnTest, MLVertexColumnBasic) {
  std::shared_ptr<MLVertexColumn> ml_col = this->build_ml_vertex_column(false);

  EXPECT_EQ(ml_col->size(), 4);
  EXPECT_EQ(ml_col->vertex_column_type(), VertexColumnType::kMultiple);
  EXPECT_EQ(ml_col->column_info(), "MLVertexColumn(0, 1)[4]");
  EXPECT_EQ(ml_col->elem_type().id(), DataTypeId::kVertex);

  EXPECT_EQ(ml_col->get_vertex(0), (VertexRecord{kLabel0, kVid0}));
  EXPECT_EQ(ml_col->get_vertex(1), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(ml_col->get_vertex(2), (VertexRecord{kLabel1, kVid1}));
  EXPECT_EQ(ml_col->get_vertex(3), (VertexRecord{kLabel1, kVid2}));
}

TEST_F(VertexColumnTest, MLVertexColumnDedup) {
  std::shared_ptr<MLVertexColumn> ml_col = this->build_ml_vertex_column(true);

  std::vector<size_t> offsets;
  ml_col->generate_dedup_offset(offsets);

  std::set<VertexRecord> expected_vertex = {
      VertexRecord{kLabel0, kVid0}, VertexRecord{kLabel0, kVid1},
      VertexRecord{kLabel1, kVid1}, VertexRecord{kLabel1, kVid2},
      VertexRecord{std::numeric_limits<label_t>::max(), kNullVid}};
  std::set<VertexRecord> actual_vertex;
  for (auto idx : offsets) {
    actual_vertex.insert(ml_col->get_vertex(idx));
  }
  EXPECT_EQ(actual_vertex, expected_vertex);
}

TEST_F(VertexColumnTest, MLVertexColumnShuffle) {
  std::shared_ptr<MLVertexColumn> ml_col = this->build_ml_vertex_column(false);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled = ml_col->shuffle(offsets);
  auto* shuffled_col = dynamic_cast<MLVertexColumn*>(shuffled.get());
  ASSERT_NE(shuffled_col, nullptr);
  EXPECT_EQ(shuffled_col->get_vertex(0), (VertexRecord{kLabel0, kVid1}));
  EXPECT_EQ(shuffled_col->get_vertex(1), (VertexRecord{kLabel0, kVid0}));
}

TEST_F(VertexColumnTest, MLVertexColumnOptionalShuffle) {
  std::shared_ptr<MLVertexColumn> ml_optional_col =
      this->build_ml_vertex_column(true);

  std::vector<size_t> offsets = {std::numeric_limits<size_t>::max(), 1, 4};
  auto shuffled = ml_optional_col->optional_shuffle(offsets);
  ASSERT_EQ(shuffled->size(), 3);

  auto* shuffled_col = dynamic_cast<MLVertexColumn*>(shuffled.get());
  ASSERT_NE(shuffled_col, nullptr);
  EXPECT_FALSE(shuffled_col->has_value(0));
  EXPECT_TRUE(shuffled_col->has_value(1));
  EXPECT_FALSE(shuffled_col->has_value(2));
}

class EdgeColumnTest : public ::testing::Test {
 protected:
  static constexpr vid_t kVid0 = 100;
  static constexpr vid_t kVid1 = 101;
  static constexpr vid_t kVid2 = 102;
  static constexpr vid_t kNullVid = std::numeric_limits<vid_t>::max();

  std::shared_ptr<SDSLEdgeColumn> build_sdsl_edge_column() { return nullptr; }
};

TEST_F(EdgeColumnTest, SDSLEdgeColumnBasic) {
  LabelTriplet label = {2, 3, 2};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  auto* sl_col = dynamic_cast<SDSLEdgeColumn*>(col_ptr.get());

  ASSERT_NE(sl_col, nullptr);
  ASSERT_EQ(sl_col->size(), 2);
  EXPECT_EQ(sl_col->edge_column_type(), EdgeColumnType::kSDSL);
  EXPECT_EQ(sl_col->elem_type().id(), DataTypeId::kEdge);
  EXPECT_EQ(sl_col->column_info(),
            "SDSLEdgeColumn: label = (2-2-3), dir = 0, size = 2");

  EXPECT_EQ(sl_col->dir(), Direction::kOut);
  EXPECT_EQ(sl_col->get_labels().size(), 1);
  EXPECT_EQ(sl_col->get_labels()[0], label);

  EdgeRecord e0 = sl_col->get_edge(0);
  EXPECT_EQ(e0.label, label);
  EXPECT_EQ(e0.dir, Direction::kOut);
  EXPECT_EQ(e0.src, kVid0);
  EXPECT_EQ(e0.dst, kVid1);
  EXPECT_EQ(e0.prop, nullptr);

  EXPECT_EQ(e0.start_node(), VertexRecord(label.src_label, kVid0));
  EXPECT_EQ(e0.end_node(), VertexRecord(label.dst_label, kVid1));
}

TEST_F(EdgeColumnTest, SDSLEdgeColumnOptional) {
  LabelTriplet label = {2, 3, 2};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label);
  EdgeRecord e = EdgeRecord{label, kVid1, kVid2, nullptr, Direction::kOut};
  builder.push_back_elem(Value::EDGE(e));
  builder.push_back_opt(kVid1, kVid2, nullptr);
  builder.push_back_null();
  auto col_ptr = builder.finish();
  auto* sl_col = dynamic_cast<SDSLEdgeColumn*>(col_ptr.get());

  ASSERT_NE(sl_col, nullptr);
  ASSERT_EQ(sl_col->size(), 3);
  EXPECT_TRUE(sl_col->is_optional());

  EdgeRecord e1 = sl_col->get_edge(2);
  EXPECT_EQ(e1.src, kNullVid);
  EXPECT_EQ(e1.dst, kNullVid);
  EXPECT_EQ(e1.prop, nullptr);
}

TEST_F(EdgeColumnTest, SDSLEdgeColumnShuffle) {
  LabelTriplet label = {2, 3, 2};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDSLEdgeColumn> sl_col =
      std::dynamic_pointer_cast<SDSLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled =
      std::dynamic_pointer_cast<SDSLEdgeColumn>(sl_col->shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord expected_e0 =
      EdgeRecord{label, kVid1, kVid2, nullptr, Direction::kOut};
  EdgeRecord expected_e1 =
      EdgeRecord{label, kVid0, kVid1, nullptr, Direction::kOut};

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1, expected_e1);
}

TEST_F(EdgeColumnTest, SDSLEdgeColumnOptionalShuffle) {
  LabelTriplet label = {2, 3, 2};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  builder.push_back_null();
  auto col_ptr = builder.finish();
  std::shared_ptr<SDSLEdgeColumn> sl_col =
      std::dynamic_pointer_cast<SDSLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {2, std::numeric_limits<size_t>::max(), 0};
  auto shuffled = std::dynamic_pointer_cast<SDSLEdgeColumn>(
      sl_col->optional_shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 3);

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0.src, kNullVid);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1.src, kNullVid);

  EdgeRecord e2 = shuffled->get_edge(2);
  EXPECT_EQ(e2.src, kVid0);
}

TEST_F(EdgeColumnTest, BDSLEdgeColumnBasic) {
  LabelTriplet label = {2, 3, 2};
  BDSLEdgeColumnBuilder builder(label);
  builder.push_back_opt(kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDSLEdgeColumn> bdsl_col =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(col_ptr);

  ASSERT_EQ(bdsl_col->size(), 2);
  EXPECT_EQ(bdsl_col->edge_column_type(), EdgeColumnType::kBDSL);
  EXPECT_EQ(bdsl_col->column_info(),
            "BDSLEdgeColumn: label = (2-2-3), size = 2");

  EdgeRecord expected_e0 =
      EdgeRecord{label, kVid0, kVid1, nullptr, Direction::kOut};
  EdgeRecord expected_e1 =
      EdgeRecord{label, kVid1, kVid0, nullptr, Direction::kIn};

  EdgeRecord e0 = bdsl_col->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = bdsl_col->get_edge(1);
  EXPECT_EQ(e1, expected_e1);
}

TEST_F(EdgeColumnTest, BDSLEdgeColumnShuffle) {
  LabelTriplet label = {2, 3, 2};
  BDSLEdgeColumnBuilder builder(label);
  builder.push_back_opt(kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDSLEdgeColumn> bdsl_col =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(bdsl_col->shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord expected_e0 =
      EdgeRecord{label, kVid1, kVid0, nullptr, Direction::kIn};
  EdgeRecord expected_e1 =
      EdgeRecord{label, kVid0, kVid1, nullptr, Direction::kOut};

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1, expected_e1);

  BDSLEdgeColumnBuilder optional_builder(label);
  optional_builder.push_back_opt(kVid0, kVid1, nullptr, Direction::kOut);
  optional_builder.push_back_null();
  optional_builder.push_back_opt(kVid1, kVid0, nullptr, Direction::kIn);

  auto optional_col_ptr = optional_builder.finish();
  std::shared_ptr<BDSLEdgeColumn> optional_ms_col =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(optional_col_ptr);

  auto optional_shuffled = std::dynamic_pointer_cast<BDSLEdgeColumn>(
      optional_ms_col->shuffle(offsets));
  ASSERT_EQ(optional_shuffled->size(), 2);

  EdgeRecord optional_e0 = optional_shuffled->get_edge(0);
  EXPECT_EQ(optional_e0.src, kNullVid);

  EdgeRecord optional_e1 = optional_shuffled->get_edge(1);
  EXPECT_EQ(optional_e1, expected_e1);
}

TEST_F(EdgeColumnTest, BDSLEdgeColumnOptionalShuffle) {
  LabelTriplet label = {2, 3, 2};
  BDSLEdgeColumnBuilder builder(label);
  builder.push_back_opt(kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(kVid1, kVid0, nullptr, Direction::kIn);
  builder.push_back_null();
  auto col_ptr = builder.finish();
  std::shared_ptr<BDSLEdgeColumn> bdsl_col =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {2, std::numeric_limits<size_t>::max(), 0};
  auto shuffled = std::dynamic_pointer_cast<BDSLEdgeColumn>(
      bdsl_col->optional_shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 3);
  EXPECT_TRUE(shuffled->is_optional());

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0.src, kNullVid);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1.src, kNullVid);

  EdgeRecord e2 = shuffled->get_edge(2);
  EXPECT_EQ(e2.src, kVid0);
}

TEST_F(EdgeColumnTest, SDMLEdgeColumnBasic) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  SDMLEdgeColumnBuilder builder(Direction::kOut, labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr);
  builder.push_back_opt(label1, kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDMLEdgeColumn> sdml_col =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(col_ptr);

  ASSERT_EQ(sdml_col->size(), 2);
  EXPECT_EQ(sdml_col->edge_column_type(), EdgeColumnType::kSDML);
  EXPECT_EQ(sdml_col->column_info(),
            "SDMLEdgeColumn: num_labels = 2, size = 2");

  EdgeRecord expected_e0 =
      EdgeRecord{label0, kVid0, kVid1, nullptr, Direction::kOut};
  EdgeRecord expected_e1 =
      EdgeRecord{label1, kVid1, kVid2, nullptr, Direction::kOut};
  EdgeRecord e0 = sdml_col->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = sdml_col->get_edge(1);
  EXPECT_EQ(e1, expected_e1);
}

TEST_F(EdgeColumnTest, SDMLEdgeColumnShuffle) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  SDMLEdgeColumnBuilder builder(Direction::kOut, labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr);
  builder.push_back_opt(label1, kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDMLEdgeColumn> sdml_col =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(sdml_col->shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord expected_e0 =
      EdgeRecord{label1, kVid1, kVid2, nullptr, Direction::kOut};
  EdgeRecord expected_e1 =
      EdgeRecord{label0, kVid0, kVid1, nullptr, Direction::kOut};

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1, expected_e1);

  SDMLEdgeColumnBuilder optional_builder(Direction::kOut, labels);
  optional_builder.push_back_opt(label0, kVid0, kVid1, nullptr);
  optional_builder.push_back_null();
  optional_builder.push_back_opt(label1, kVid1, kVid0, nullptr);

  auto optional_col_ptr = optional_builder.finish();
  std::shared_ptr<SDMLEdgeColumn> optional_ms_col =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(optional_col_ptr);

  auto optional_shuffled = std::dynamic_pointer_cast<SDMLEdgeColumn>(
      optional_ms_col->shuffle(offsets));
  ASSERT_EQ(optional_shuffled->size(), 2);

  EdgeRecord optional_e0 = optional_shuffled->get_edge(0);
  EXPECT_EQ(optional_e0.src, kNullVid);

  EdgeRecord optional_e1 = optional_shuffled->get_edge(1);
  EXPECT_EQ(optional_e1, expected_e1);
}

TEST_F(EdgeColumnTest, SDMLEdgeColumnOptionalShuffle) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  SDMLEdgeColumnBuilder builder(Direction::kOut, labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr);
  builder.push_back_opt(label1, kVid1, kVid2, nullptr);
  builder.push_back_null();
  auto col_ptr = builder.finish();
  std::shared_ptr<SDMLEdgeColumn> sdml_col =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {2, std::numeric_limits<size_t>::max(), 0};
  auto shuffled = std::dynamic_pointer_cast<SDMLEdgeColumn>(
      sdml_col->optional_shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 3);

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0.src, kNullVid);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1.src, kNullVid);

  EdgeRecord e2 = shuffled->get_edge(2);
  EXPECT_EQ(e2.src, kVid0);
}

TEST_F(EdgeColumnTest, BDMLEdgeColumnBasic) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  BDMLEdgeColumnBuilder builder(labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(label1, kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDMLEdgeColumn> bdml_col =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(col_ptr);

  ASSERT_EQ(bdml_col->size(), 2);
  EXPECT_EQ(bdml_col->edge_column_type(), EdgeColumnType::kBDML);
  EXPECT_EQ(bdml_col->column_info(),
            "BDMLEdgeColumn: num_labels = 2, size = 2");

  EdgeRecord expected_e0 =
      EdgeRecord{label0, kVid0, kVid1, nullptr, Direction::kOut};
  EdgeRecord expected_e1 =
      EdgeRecord{label1, kVid1, kVid0, nullptr, Direction::kIn};
  EdgeRecord e0 = bdml_col->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = bdml_col->get_edge(1);
  EXPECT_EQ(e1, expected_e1);
}

TEST_F(EdgeColumnTest, BDMLEdgeColumnShuffle) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  BDMLEdgeColumnBuilder builder(labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(label1, kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDMLEdgeColumn> bdml_col =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(bdml_col->shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord expected_e0 =
      EdgeRecord{label1, kVid1, kVid0, nullptr, Direction::kIn};
  EdgeRecord expected_e1 =
      EdgeRecord{label0, kVid0, kVid1, nullptr, Direction::kOut};

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1, expected_e1);

  BDMLEdgeColumnBuilder optional_builder(labels);
  optional_builder.push_back_opt(label0, kVid0, kVid1, nullptr,
                                 Direction::kOut);
  optional_builder.push_back_null();
  optional_builder.push_back_opt(label1, kVid1, kVid0, nullptr, Direction::kIn);

  auto optional_col_ptr = optional_builder.finish();
  std::shared_ptr<BDMLEdgeColumn> optional_ms_col =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(optional_col_ptr);

  auto optional_shuffled = std::dynamic_pointer_cast<BDMLEdgeColumn>(
      optional_ms_col->shuffle(offsets));
  ASSERT_EQ(optional_shuffled->size(), 2);

  EdgeRecord optional_e0 = optional_shuffled->get_edge(0);
  EXPECT_EQ(optional_e0.src, kNullVid);

  EdgeRecord optional_e1 = optional_shuffled->get_edge(1);
  EXPECT_EQ(optional_e1, expected_e1);
}

TEST_F(EdgeColumnTest, BDMLEdgeColumnOptionalShuffle) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  BDMLEdgeColumnBuilder builder(labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(label1, kVid1, kVid0, nullptr, Direction::kIn);
  builder.push_back_null();
  auto col_ptr = builder.finish();
  std::shared_ptr<BDMLEdgeColumn> bdml_col =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {2, std::numeric_limits<size_t>::max(), 0};
  auto shuffled = std::dynamic_pointer_cast<BDMLEdgeColumn>(
      bdml_col->optional_shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 3);

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0.src, kNullVid);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1.src, kNullVid);

  EdgeRecord e2 = shuffled->get_edge(2);
  EXPECT_EQ(e2.src, kVid0);
}

TEST_F(EdgeColumnTest, MSEdgeColumnFromBuilder) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  MSEdgeColumnBuilder builder;
  builder.reserve(3);
  builder.start_label_dir(label0, Direction::kOut);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.start_label_dir(label1, Direction::kIn);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  builder.push_back_null();
  auto col_ptr = builder.finish();
  std::shared_ptr<MSEdgeColumn> ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(col_ptr);

  ASSERT_EQ(ms_col->size(), 3);
  EXPECT_EQ(ms_col->edge_column_type(), EdgeColumnType::kMS);
  EXPECT_EQ(ms_col->column_info(),
            "OptionalMSEdgeColumn: num_labels = 2, size = 3");

  EdgeRecord expected_e0 =
      EdgeRecord{label0, kVid0, kVid1, nullptr, Direction::kOut};
  EdgeRecord expected_e1 =
      EdgeRecord{label1, kVid1, kVid2, nullptr, Direction::kIn};
  EdgeRecord e0 = ms_col->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = ms_col->get_edge(1);
  EXPECT_EQ(e1, expected_e1);
}

TEST_F(EdgeColumnTest, MSEdgeColumnSingleLabelShuffle) {
  LabelTriplet label = {2, 3, 2};
  MSEdgeColumnBuilder builder;
  builder.start_label_dir(label, Direction::kOut);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.start_label_dir(label, Direction::kIn);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<MSEdgeColumn> ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(ms_col->shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord expected_e0 =
      EdgeRecord{label, kVid1, kVid2, nullptr, Direction::kIn};
  EdgeRecord expected_e1 =
      EdgeRecord{label, kVid0, kVid1, nullptr, Direction::kOut};

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1, expected_e1);

  MSEdgeColumnBuilder optional_builder;
  optional_builder.start_label_dir(label, Direction::kOut);
  optional_builder.push_back_opt(kVid0, kVid1, nullptr);
  optional_builder.push_back_null();
  optional_builder.start_label_dir(label, Direction::kIn);
  optional_builder.push_back_opt(kVid1, kVid2, nullptr);

  auto optional_col_ptr = optional_builder.finish();
  std::shared_ptr<MSEdgeColumn> optional_ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(optional_col_ptr);

  auto optional_shuffled = std::dynamic_pointer_cast<BDSLEdgeColumn>(
      optional_ms_col->shuffle(offsets));
  ASSERT_EQ(optional_shuffled->size(), 2);

  EdgeRecord optional_e0 = optional_shuffled->get_edge(0);
  EXPECT_EQ(optional_e0.src, kNullVid);

  EdgeRecord optional_e1 = optional_shuffled->get_edge(1);
  EXPECT_EQ(optional_e1, expected_e1);
}

TEST_F(EdgeColumnTest, MSEdgeColumnShuffle) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  MSEdgeColumnBuilder builder;
  builder.start_label_dir(label0, Direction::kOut);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.start_label_dir(label1, Direction::kIn);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<MSEdgeColumn> ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {1, 0};
  auto shuffled =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(ms_col->shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord expected_e0 =
      EdgeRecord{label1, kVid1, kVid2, nullptr, Direction::kIn};
  EdgeRecord expected_e1 =
      EdgeRecord{label0, kVid0, kVid1, nullptr, Direction::kOut};

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0, expected_e0);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1, expected_e1);

  MSEdgeColumnBuilder optional_builder;
  optional_builder.start_label_dir(label0, Direction::kOut);
  optional_builder.push_back_opt(kVid0, kVid1, nullptr);
  optional_builder.push_back_null();
  optional_builder.start_label_dir(label1, Direction::kIn);
  optional_builder.push_back_opt(kVid1, kVid2, nullptr);

  auto optional_col_ptr = optional_builder.finish();
  std::shared_ptr<MSEdgeColumn> optional_ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(optional_col_ptr);

  auto optional_shuffled = std::dynamic_pointer_cast<BDMLEdgeColumn>(
      optional_ms_col->shuffle(offsets));
  ASSERT_EQ(optional_shuffled->size(), 2);

  EdgeRecord optional_e0 = optional_shuffled->get_edge(0);
  EXPECT_EQ(optional_e0.src, kNullVid);

  EdgeRecord optional_e1 = optional_shuffled->get_edge(1);
  EXPECT_EQ(optional_e1, expected_e1);
}

TEST_F(EdgeColumnTest, MSEdgeColumnOptionalShuffle) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  MSEdgeColumnBuilder builder;
  builder.start_label_dir(label0, Direction::kOut);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.start_label_dir(label1, Direction::kIn);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<MSEdgeColumn> ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(col_ptr);

  std::vector<size_t> offsets = {std::numeric_limits<size_t>::max(), 0};
  auto shuffled = std::dynamic_pointer_cast<BDMLEdgeColumn>(
      ms_col->optional_shuffle(offsets));
  ASSERT_EQ(shuffled->size(), 2);

  EdgeRecord e0 = shuffled->get_edge(0);
  EXPECT_EQ(e0.src, kNullVid);

  EdgeRecord e1 = shuffled->get_edge(1);
  EXPECT_EQ(e1.src, kVid0);

  MSEdgeColumnBuilder single_builder;
  single_builder.start_label_dir(label0, Direction::kOut);
  single_builder.push_back_opt(kVid0, kVid1, nullptr);
  single_builder.start_label_dir(label0, Direction::kIn);
  single_builder.push_back_opt(kVid1, kVid2, nullptr);
  auto single_col_ptr = single_builder.finish();
  std::shared_ptr<MSEdgeColumn> single_ms_col =
      std::dynamic_pointer_cast<MSEdgeColumn>(single_col_ptr);

  auto single_shuffled = std::dynamic_pointer_cast<BDSLEdgeColumn>(
      single_ms_col->optional_shuffle(offsets));
  ASSERT_EQ(single_shuffled->size(), 2);

  EdgeRecord single_e0 = single_shuffled->get_edge(0);
  EXPECT_EQ(single_e0.src, kNullVid);

  EdgeRecord single_e1 = single_shuffled->get_edge(1);
  EXPECT_EQ(single_e1.src, kVid0);
}

TEST_F(EdgeColumnTest, ForeachEdgeSDSL) {
  LabelTriplet label0 = {2, 3, 2};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label0);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDSLEdgeColumn> sdsl_col =
      std::dynamic_pointer_cast<SDSLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  foreach_edge(*sdsl_col,
               [&](size_t idx, const LabelTriplet& label, Direction dir,
                   vid_t src, vid_t dst, const void* prop) {
                 EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
                 collected.push_back(e);
               });

  ASSERT_EQ(collected.size(), 1);
  EXPECT_EQ(collected[0], sdsl_col->get_edge(0));
}

TEST_F(EdgeColumnTest, ForeachEdgeBDSL) {
  LabelTriplet label0 = {2, 3, 2};
  BDSLEdgeColumnBuilder builder(label0);
  builder.push_back_opt(kVid0, kVid1, nullptr, Direction::kOut);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDSLEdgeColumn> bdsl_col =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  foreach_edge(*bdsl_col,
               [&](size_t idx, const LabelTriplet& label, Direction dir,
                   vid_t src, vid_t dst, const void* prop) {
                 EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
                 collected.push_back(e);
               });

  ASSERT_EQ(collected.size(), 1);
  EXPECT_EQ(collected[0], bdsl_col->get_edge(0));
}

TEST_F(EdgeColumnTest, ForeachEdgeSDML) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  SDMLEdgeColumnBuilder builder(Direction::kOut, labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr);
  builder.push_back_opt(label1, kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDMLEdgeColumn> sdml_col =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  foreach_edge(*sdml_col,
               [&](size_t idx, const LabelTriplet& label, Direction dir,
                   vid_t src, vid_t dst, const void* prop) {
                 EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
                 collected.push_back(e);
               });

  ASSERT_EQ(collected.size(), 2);
  EXPECT_EQ(collected[0], sdml_col->get_edge(0));
}

TEST_F(EdgeColumnTest, ForeachEdgeBDML) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  BDMLEdgeColumnBuilder builder(labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(label1, kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDMLEdgeColumn> bdml_col =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  foreach_edge(*bdml_col,
               [&](size_t idx, const LabelTriplet& label, Direction dir,
                   vid_t src, vid_t dst, const void* prop) {
                 EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
                 collected.push_back(e);
               });

  ASSERT_EQ(collected.size(), 2);
  EXPECT_EQ(collected[0], bdml_col->get_edge(0));
}

TEST_F(EdgeColumnTest, ForeachEdgeFilterSDSL) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label0);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDSLEdgeColumn> sdsl_col =
      std::dynamic_pointer_cast<SDSLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  // Filter for different label -> should include all
  std::vector<std::pair<LabelTriplet, Direction>> filter = {
      {label1, Direction::kOut}};
  foreach_edge(
      *sdsl_col,
      [&](size_t idx, const LabelTriplet& label, Direction dir, vid_t src,
          vid_t dst, const void* prop) {
        EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
        collected.push_back(e);
      },
      filter);

  ASSERT_EQ(collected.size(), 1);
  EXPECT_EQ(collected[0], sdsl_col->get_edge(0));
}

TEST_F(EdgeColumnTest, ForeachEdgeFilterBDSL) {
  LabelTriplet label0 = {2, 3, 2};
  BDSLEdgeColumnBuilder builder(label0);
  builder.push_back_opt(kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDSLEdgeColumn> bdsl_col =
      std::dynamic_pointer_cast<BDSLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  // Filter to exclude in-edges
  std::vector<std::pair<LabelTriplet, Direction>> filter = {
      {label0, Direction::kOut}};
  foreach_edge(
      *bdsl_col,
      [&](size_t idx, const LabelTriplet& label, Direction dir, vid_t src,
          vid_t dst, const void* prop) {
        EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
        collected.push_back(e);
      },
      filter);

  // Should only have out-edge
  ASSERT_EQ(collected.size(), 1);
  EXPECT_EQ(collected[0].dir, Direction::kOut);
}

TEST_F(EdgeColumnTest, ForeachEdgeFilterSDML) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  SDMLEdgeColumnBuilder builder(Direction::kOut, labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr);
  builder.push_back_opt(label1, kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDMLEdgeColumn> sdml_col =
      std::dynamic_pointer_cast<SDMLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  // Filter for different label -> should include all
  std::vector<std::pair<LabelTriplet, Direction>> filter = {
      {label1, Direction::kOut}};
  foreach_edge(
      *sdml_col,
      [&](size_t idx, const LabelTriplet& label, Direction dir, vid_t src,
          vid_t dst, const void* prop) {
        EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
        collected.push_back(e);
      },
      filter);

  ASSERT_EQ(collected.size(), 1);
  EXPECT_EQ(collected[0], sdml_col->get_edge(0));
}

TEST_F(EdgeColumnTest, ForeachEdgeFilterBDML) {
  LabelTriplet label0 = {2, 3, 2};
  LabelTriplet label1 = {2, 3, 3};
  std::vector<LabelTriplet> labels = {label0, label1};
  BDMLEdgeColumnBuilder builder(labels);
  builder.push_back_opt(label0, kVid0, kVid1, nullptr, Direction::kOut);
  builder.push_back_opt(label1, kVid1, kVid0, nullptr, Direction::kIn);
  auto col_ptr = builder.finish();
  std::shared_ptr<BDMLEdgeColumn> bdml_col =
      std::dynamic_pointer_cast<BDMLEdgeColumn>(col_ptr);

  std::vector<EdgeRecord> collected;
  // Filter to exclude in-edges
  std::vector<std::pair<LabelTriplet, Direction>> filter = {
      {label0, Direction::kOut}};
  foreach_edge(
      *bdml_col,
      [&](size_t idx, const LabelTriplet& label, Direction dir, vid_t src,
          vid_t dst, const void* prop) {
        EdgeRecord e = EdgeRecord{label, src, dst, prop, dir};
        collected.push_back(e);
      },
      filter);

  // Should only have out-edge
  ASSERT_EQ(collected.size(), 1);
  EXPECT_EQ(collected[0].dir, Direction::kIn);
}

TEST_F(EdgeColumnTest, SDSLEdgeColumnDedup) {
  LabelTriplet label0 = {2, 3, 2};
  SDSLEdgeColumnBuilder builder(Direction::kOut, label0);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.push_back_opt(kVid0, kVid1, nullptr);
  builder.push_back_opt(kVid1, kVid2, nullptr);
  auto col_ptr = builder.finish();
  std::shared_ptr<SDSLEdgeColumn> sdsl_col =
      std::dynamic_pointer_cast<SDSLEdgeColumn>(col_ptr);

  std::vector<size_t> offsets;
  sdsl_col->generate_dedup_offset(offsets);

  // Should have 2 unique edges
  EXPECT_EQ(offsets.size(), 2);
}

class PathColumnTest : public ::testing::Test {};

TEST_F(PathColumnTest, PathColumnBasic) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder;

  builder.push_back_opt(p1);
  builder.push_back_elem(Value::PATH(p2));
  auto col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->column_info(), "PathColumn[2]");
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kPath);
  EXPECT_EQ(col->size(), 2);
  EXPECT_EQ(col->get_path(0), p1);
  EXPECT_EQ(col->get_path(1), p2);
  EXPECT_EQ(col->get_path(0).length(), 2);

  Value elem0 = col->get_elem(0);
  EXPECT_EQ(PathValue::Get(elem0), p1);
}

TEST_F(PathColumnTest, PathColumnShuffle) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder;
  builder.push_back_opt(p1);
  builder.push_back_opt(p2);
  auto base_col = builder.finish();

  std::vector<size_t> offsets = {1, 0};
  auto shuffled = base_col->shuffle(offsets);
  ASSERT_EQ(shuffled->size(), 2);

  EXPECT_EQ(PathValue::Get(shuffled->get_elem(1)), p1);
  EXPECT_EQ(PathValue::Get(shuffled->get_elem(0)), p2);
}

TEST_F(PathColumnTest, PathColumnOptionalShuffle) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder;
  builder.push_back_opt(p1);
  builder.push_back_opt(p2);
  auto base_col = builder.finish();

  std::vector<size_t> offsets = {1, std::numeric_limits<size_t>::max(), 0};
  auto shuffled = base_col->optional_shuffle(offsets);
  ASSERT_EQ(shuffled->size(), 3);

  auto opt_col = std::dynamic_pointer_cast<PathColumn>(shuffled);
  ASSERT_NE(opt_col, nullptr);
  EXPECT_TRUE(opt_col->has_value(0));
  EXPECT_FALSE(opt_col->has_value(1));
  EXPECT_TRUE(opt_col->has_value(2));

  EXPECT_EQ(opt_col->get_path(0), p2);
  EXPECT_EQ(opt_col->get_path(2), p1);
}

TEST_F(PathColumnTest, PathColumnDedup) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder;
  builder.push_back_opt(p1);
  builder.push_back_opt(p1);
  builder.push_back_opt(p2);
  builder.push_back_opt(p2);
  auto col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  std::vector<size_t> offsets;
  col->generate_dedup_offset(offsets);

  EXPECT_EQ(offsets.size(), 2);  // unique paths

  std::set<Path> unique;
  for (auto idx : offsets) {
    unique.insert(col->get_path(idx));
  }
  EXPECT_EQ(unique.size(), 2);
}

TEST_F(PathColumnTest, OptionalPathColumnBasic) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder(true);
  builder.push_back_opt(p1);
  builder.push_back_null();
  builder.push_back_elem(Value::PATH(p2));

  auto col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 3);
  EXPECT_EQ(col->column_info(), "PathColumn[3]");
  EXPECT_EQ(col->column_type(), ContextColumnType::kPath);
  EXPECT_EQ(col->elem_type().id(), DataTypeId::kPath);
  EXPECT_TRUE(col->is_optional());
  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
  EXPECT_TRUE(col->has_value(2));

  EXPECT_EQ(PathValue::Get(col->get_elem(0)), p1);
  EXPECT_TRUE(col->get_elem(1).IsNull());
  EXPECT_EQ(PathValue::Get(col->get_elem(2)), p2);
}

TEST_F(PathColumnTest, OptionalPathColumnShuffle) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids = {1, 2, 3};
  std::vector<std::pair<Direction, const void*>> edge_datas;
  for (size_t i = 0; i < 3; i++) {
    edge_datas.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p(v_label, e_label, vids, edge_datas);

  PathColumnBuilder builder(true);
  builder.push_back_opt(p);
  builder.push_back_null();

  auto base_col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  std::vector<size_t> offsets = {1, 0};
  auto shuffled = base_col->shuffle(offsets);
  auto opt_col = std::dynamic_pointer_cast<PathColumn>(shuffled);

  ASSERT_NE(opt_col, nullptr);
  EXPECT_FALSE(opt_col->has_value(0));
  EXPECT_TRUE(opt_col->has_value(1));
  EXPECT_EQ(opt_col->get_path(1), p);
}

TEST_F(PathColumnTest, OptionalPathColumnPushBackNull) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids = {1, 2, 3};
  std::vector<std::pair<Direction, const void*>> edge_datas;
  for (size_t i = 0; i < 3; i++) {
    edge_datas.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p(v_label, e_label, vids, edge_datas);

  PathColumnBuilder builder(true);
  builder.push_back_opt(p);
  builder.push_back_null();

  auto col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->size(), 2);
  EXPECT_TRUE(col->has_value(0));
  EXPECT_FALSE(col->has_value(1));
}

TEST_F(PathColumnTest, PathColumnForeach) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder;
  builder.push_back_opt(p1);
  builder.push_back_opt(p2);
  auto col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  std::vector<Path> collected;
  col->foreach_path(
      [&](size_t idx, const Path& path) { collected.push_back(path); });

  ASSERT_EQ(collected.size(), 2);
  EXPECT_EQ(collected[0], p1);
  EXPECT_EQ(collected[1], p2);
}

TEST_F(PathColumnTest, OptionalPathColumnForeach) {
  label_t v_label = 0;
  label_t e_label = 1;

  std::vector<vid_t> vids1 = {1, 2, 3};
  std::vector<vid_t> vids2 = {4, 5};
  std::vector<std::pair<Direction, const void*>> edge_datas1, edge_datas2;
  for (size_t i = 0; i < 3; i++) {
    edge_datas1.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }
  for (size_t i = 0; i < 2; i++) {
    edge_datas2.emplace_back(std::make_pair(Direction::kOut, nullptr));
  }

  Path p1(v_label, e_label, vids1, edge_datas1);
  Path p2(v_label, e_label, vids2, edge_datas2);

  PathColumnBuilder builder(true);
  builder.push_back_opt(p1);
  builder.push_back_null();

  auto col = std::dynamic_pointer_cast<PathColumn>(builder.finish());

  std::vector<std::pair<size_t, Path>> collected;
  col->foreach_path(
      [&](size_t idx, const Path& path) { collected.emplace_back(idx, path); });

  // foreach_path iterates over all, regardless of validity
  ASSERT_EQ(collected.size(), 2);
  EXPECT_EQ(collected[0].second, p1);
}

class ArrowContextColumnTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(ArrowContextColumnTest, ArrowArrayContextColumnBasic) {
  std::vector<std::shared_ptr<arrow::Array>> columns;
  ArrowArrayContextColumn col = ArrowArrayContextColumn(columns);

  EXPECT_EQ(col.column_info(), "ArrowArrayContextColumn");
  EXPECT_EQ(col.size(), 0);
  EXPECT_EQ(col.column_type(), ContextColumnType::kArrowArray);
  EXPECT_EQ(col.is_optional(), false);
  EXPECT_EQ(col.GetColumns().size(), 0);
  EXPECT_EQ(col.GetArrowType(), arrow::null());
}

TEST_F(ArrowContextColumnTest, ArrowStreamContextColumnBasic) {
  const char* var = std::getenv("TEST_PATH");
  std::string test_path = var ? var : "/workspaces/neug/tests";
  std::string resource_path = test_path + "/execution/resources";
  std::string file_path = resource_path + "/test.csv";
  std::vector<DataType> column_types;
  column_types.emplace_back(DataTypeId::kInt32);
  column_types.emplace_back(DataTypeId::kVarchar);
  column_types.emplace_back(DataTypeId::kInt32);
  std::unordered_map<std::string, std::string> options;
  options.insert({"HEADER", "TRUE"});
  options.insert({"DELIM", "|"});
  options.insert({"STREAM_READER", "true"});

  auto stream_suppliers =
      ops::create_csv_record_suppliers(file_path, column_types, options);
  ArrowStreamContextColumnBuilder builder(stream_suppliers);
  auto arrow_stream_context_column =
      std::dynamic_pointer_cast<ArrowStreamContextColumn>(builder.finish());
  EXPECT_EQ(arrow_stream_context_column->column_info(),
            "ArrowStreamContextColumn");
  EXPECT_EQ(arrow_stream_context_column->size(), 1);
  EXPECT_EQ(arrow_stream_context_column->column_type(),
            ContextColumnType::kArrowStream);
  arrow_stream_context_column->GetSuppliers();
}

}  // namespace test
}  // namespace execution
}  // namespace neug