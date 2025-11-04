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
#include <string_view>

#include "neug/storages/graph/edge_table.h"
#include "neug/storages/loader/loader_utils.h"
#include "utils.h"

class GeneratedRecordBatchSupplier : public gs::IRecordBatchSupplier {
 public:
  GeneratedRecordBatchSupplier(
      std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches)
      : batches_(std::move(batches)) {}
  ~GeneratedRecordBatchSupplier() override = default;

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override {
    if (batches_.empty()) {
      return nullptr;
    } else {
      auto batch = batches_.back();
      batches_.pop_back();
      return batch;
    }
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};

class EdgeTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("edge_table_" + std::to_string(::getpid()) + "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    std::filesystem::create_directories(SnapshotDirectory());
    std::filesystem::create_directories(WorkDirectory());
    std::filesystem::create_directories(WorkDirectory() / "runtime/tmp");

    schema_.AddVertexLabel("person", {}, {},
                           {std::make_tuple(gs::PropertyType::kInt64, "id", 0)},
                           {gs::StorageStrategy::kMem},
                           static_cast<size_t>(1) << 32, "person vertex label");
    schema_.AddVertexLabel(
        "comment", {}, {}, {std::make_tuple(gs::PropertyType::kInt64, "id", 0)},
        {gs::StorageStrategy::kMem}, static_cast<size_t>(1) << 32,
        "comment vertex label");
    schema_.AddEdgeLabel(
        "person", "comment", "create1", {gs::PropertyType::kInt32}, {"data"},
        {gs::StorageStrategy::kMem}, gs::EdgeStrategy::kMultiple,
        gs::EdgeStrategy::kMultiple, true, true, false,
        "person creates comment edge");
    schema_.AddEdgeLabel(
        "person", "comment", "create2", {gs::PropertyType::kStringView},
        {"data"}, {gs::StorageStrategy::kMem}, gs::EdgeStrategy::kMultiple,
        gs::EdgeStrategy::kMultiple, true, true, false,
        "person creates comment edge");
    schema_.AddEdgeLabel(
        "person", "comment", "create3",
        {gs::PropertyType::kStringView, gs::PropertyType::kInt32},
        {"data0", "data1"},
        {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem},
        gs::EdgeStrategy::kMultiple, gs::EdgeStrategy::kMultiple, true, true,
        false, "person creates comment edge with two properties");
    src_label_ = schema_.get_vertex_label_id("person");
    dst_label_ = schema_.get_vertex_label_id("comment");
    edge_label_int_ = schema_.get_edge_label_id("create1");
    edge_label_str_ = schema_.get_edge_label_id("create2");
    edge_label_str_int_ = schema_.get_edge_label_id("create3");
  }
  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  std::filesystem::path WorkDirectory() const { return temp_dir_ / "work"; }
  std::filesystem::path SnapshotDirectory() const {
    return WorkDirectory() / "checkpoint";
  }

  void InitIndexers(gs::vid_t src_num, gs::vid_t dst_num) {
    gs::IdIndexer<int64_t, gs::vid_t> src_input, dst_input;
    for (gs::vid_t i = 0; i < src_num; ++i) {
      gs::vid_t lid;
      src_input.add(static_cast<int64_t>(i), lid);
    }
    for (gs::vid_t i = 0; i < dst_num; ++i) {
      gs::vid_t lid;
      dst_input.add(static_cast<int64_t>(i), lid);
    }
    gs::build_lf_indexer<int64_t, gs::vid_t>(
        src_input, "src_indexer", src_indexer, SnapshotDirectory().string(),
        WorkDirectory().string(), gs::PropertyType::Int64());
    gs::build_lf_indexer<int64_t, gs::vid_t>(
        dst_input, "dst_indexer", dst_indexer, SnapshotDirectory().string(),
        WorkDirectory().string(), gs::PropertyType::Int64());
  }

  void ConstructEdgeTable(gs::label_t src_label, gs::label_t dst_label,
                          gs::label_t edge_label) {
    edge_table = std::make_unique<gs::EdgeTable>(
        schema_.get_edge_schema(src_label, dst_label, edge_label));
  }

  void OpenEdgeTable() { edge_table->Open(WorkDirectory().string()); }

  void OpenEdgeTableInMemory(size_t src_v_cap, size_t dst_v_cap) {
    edge_table->OpenInMemory(SnapshotDirectory().string(), src_v_cap,
                             dst_v_cap);
  }

  void BatchInsert(std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) {
    auto supplier =
        std::make_shared<GeneratedRecordBatchSupplier>(std::move(batches));
    edge_table->BatchAddEdges(src_indexer, dst_indexer, supplier);
  }

  void OutputOutgoingEndpoints(std::vector<int64_t>& srcs,
                               std::vector<int64_t>& dsts, gs::timestamp_t ts) {
    auto view = edge_table->get_outgoing_view(ts);
    gs::vid_t vnum = src_indexer.size();
    for (gs::vid_t i = 0; i < vnum; ++i) {
      auto es = view.get_edges(i);
      int64_t src_oid = src_indexer.get_key(i).as_int64();
      for (auto it = es.begin(); it != es.end(); ++it) {
        int64_t dst_oid = dst_indexer.get_key(it.get_vertex()).as_int64();
        srcs.push_back(src_oid);
        dsts.push_back(dst_oid);
      }
    }
  }

  void OutputIncomingEndpoints(std::vector<int64_t>& srcs,
                               std::vector<int64_t>& dsts, gs::timestamp_t ts) {
    auto view = edge_table->get_incoming_view(ts);
    gs::vid_t vnum = dst_indexer.size();
    for (gs::vid_t i = 0; i < vnum; ++i) {
      auto es = view.get_edges(i);
      int64_t dst_oid = dst_indexer.get_key(i).as_int64();
      for (auto it = es.begin(); it != es.end(); ++it) {
        int64_t src_oid = src_indexer.get_key(it.get_vertex()).as_int64();
        srcs.push_back(src_oid);
        dsts.push_back(dst_oid);
      }
    }
  }

  template <typename T>
  void OutputOutgoingEdgeData(std::vector<T>& data, gs::timestamp_t ts,
                              int prop_id) {
    auto ed_accessor = edge_table->get_edge_data_accessor(prop_id);
    auto view = edge_table->get_outgoing_view(ts);
    gs::vid_t vnum = src_indexer.size();
    for (gs::vid_t i = 0; i < vnum; ++i) {
      auto es = view.get_edges(i);
      for (auto it = es.begin(); it != es.end(); ++it) {
        data.push_back(ed_accessor.get_typed_data<T>(it));
      }
    }
  }

  template <typename T>
  void OutputIncomingEdgeData(std::vector<T>& data, gs::timestamp_t ts,
                              int prop_id) {
    auto ed_accessor = edge_table->get_edge_data_accessor(prop_id);
    auto view = edge_table->get_incoming_view(ts);
    gs::vid_t vnum = dst_indexer.size();
    for (gs::vid_t i = 0; i < vnum; ++i) {
      auto es = view.get_edges(i);
      for (auto it = es.begin(); it != es.end(); ++it) {
        data.push_back(ed_accessor.get_typed_data<T>(it));
      }
    }
  }

  std::unique_ptr<gs::EdgeTable> edge_table = nullptr;
  gs::LFIndexer<gs::vid_t> src_indexer;
  gs::LFIndexer<gs::vid_t> dst_indexer;
  gs::Schema schema_;
  gs::label_t src_label_, dst_label_, edge_label_int_, edge_label_str_,
      edge_label_str_int_;

 private:
  std::filesystem::path temp_dir_;
  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }
};

TEST_F(EdgeTableTest, TestBundledInt32) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  int64_t src_num = 1000;
  int64_t dst_num = 1000;
  size_t edge_num = 20000;

  auto src_list = generate_random_vertices<int64_t>(src_num, edge_num);
  auto dst_list = generate_random_vertices<int64_t>(dst_num, edge_num);

  auto data_list = generate_random_data<int>(edge_num);

  auto src_arrs = convert_to_arrow_arrays(src_list, 10);
  auto dst_arrs = convert_to_arrow_arrays(dst_list, 10);
  auto data_arrs = convert_to_arrow_arrays(data_list, 10);

  auto batches = convert_to_record_batches({"src", "dst", "data"},
                                           {src_arrs, dst_arrs, data_arrs});

  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_int_);
  this->OpenEdgeTable();
  this->BatchInsert(std::move(batches));

  std::vector<std::tuple<int64_t, int64_t, int>> input;
  for (size_t i = 0; i < edge_num; ++i) {
    input.emplace_back(src_list[i], dst_list[i], data_list[i]);
  }
  std::sort(input.begin(), input.end());
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputOutgoingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<int> datas;
    this->OutputOutgoingEdgeData<int>(datas, 0, 0);
    ASSERT_EQ(datas.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, int>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], datas[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
    }
  }
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputIncomingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<int> datas;
    this->OutputIncomingEdgeData<int>(datas, 0, 0);
    ASSERT_EQ(datas.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, int>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], datas[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
    }
  }
}

TEST_F(EdgeTableTest, TestSeperatedString) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  int64_t src_num = 1000;
  int64_t dst_num = 1000;
  size_t edge_num = 20000;

  auto src_list = generate_random_vertices<int64_t>(src_num, edge_num);
  auto dst_list = generate_random_vertices<int64_t>(dst_num, edge_num);

  auto data_list = generate_random_data<std::string>(edge_num);

  auto src_arrs = convert_to_arrow_arrays(src_list, 10);
  auto dst_arrs = convert_to_arrow_arrays(dst_list, 10);
  auto data_arrs = convert_to_arrow_arrays(data_list, 10);

  auto batches = convert_to_record_batches({"src", "dst", "data"},
                                           {src_arrs, dst_arrs, data_arrs});

  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_);
  this->OpenEdgeTable();
  this->BatchInsert(std::move(batches));

  std::vector<std::tuple<int64_t, int64_t, std::string>> input;
  for (size_t i = 0; i < edge_num; ++i) {
    input.emplace_back(src_list[i], dst_list[i], data_list[i]);
  }
  std::sort(input.begin(), input.end());
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputOutgoingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<std::string_view> datas;
    this->OutputOutgoingEdgeData<std::string_view>(datas, 0, 0);
    ASSERT_EQ(datas.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, std::string_view>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], datas[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
    }
  }
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputIncomingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<std::string_view> datas;
    this->OutputIncomingEdgeData<std::string_view>(datas, 0, 0);
    ASSERT_EQ(datas.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, std::string_view>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], datas[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
    }
  }
}

TEST_F(EdgeTableTest, TestSeperatedIntString) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  int64_t src_num = 1000;
  int64_t dst_num = 1000;
  size_t edge_num = 20000;

  auto src_list = generate_random_vertices<int64_t>(src_num, edge_num);
  auto dst_list = generate_random_vertices<int64_t>(dst_num, edge_num);

  auto data_list0 = generate_random_data<std::string>(edge_num);
  auto data_list1 = generate_random_data<int>(edge_num);

  auto src_arrs = convert_to_arrow_arrays(src_list, 10);
  auto dst_arrs = convert_to_arrow_arrays(dst_list, 10);
  auto data_arrs0 = convert_to_arrow_arrays(data_list0, 10);
  auto data_arrs1 = convert_to_arrow_arrays(data_list1, 10);

  auto batches =
      convert_to_record_batches({"src", "dst", "prop0", "prop1"},
                                {src_arrs, dst_arrs, data_arrs0, data_arrs1});

  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTable();
  this->BatchInsert(std::move(batches));

  std::vector<std::tuple<int64_t, int64_t, std::string, int>> input;
  for (size_t i = 0; i < edge_num; ++i) {
    input.emplace_back(src_list[i], dst_list[i], data_list0[i], data_list1[i]);
  }
  std::sort(input.begin(), input.end());
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputOutgoingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<std::string_view> data0;
    std::vector<int> data1;
    this->OutputOutgoingEdgeData<std::string_view>(data0, 0, 0);
    ASSERT_EQ(data0.size(), edge_num);
    this->OutputOutgoingEdgeData<int>(data1, 0, 1);
    ASSERT_EQ(data1.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, std::string_view, int>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], data0[i], data1[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
      EXPECT_EQ(std::get<3>(input[i]), std::get<3>(output[i]));
    }
  }
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputIncomingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<std::string_view> data0;
    this->OutputIncomingEdgeData<std::string_view>(data0, 0, 0);
    ASSERT_EQ(data0.size(), edge_num);
    std::vector<int> data1;
    this->OutputIncomingEdgeData<int>(data1, 0, 1);
    ASSERT_EQ(data1.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, std::string_view, int>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], data0[i], data1[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
      EXPECT_EQ(std::get<3>(input[i]), std::get<3>(output[i]));
    }
  }

  this->edge_table->Dump(this->SnapshotDirectory().string());
  this->edge_table.reset();

  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTable();
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputOutgoingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<std::string_view> data0;
    std::vector<int> data1;
    this->OutputOutgoingEdgeData<std::string_view>(data0, 0, 0);
    ASSERT_EQ(data0.size(), edge_num);
    this->OutputOutgoingEdgeData<int>(data1, 0, 1);
    ASSERT_EQ(data1.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, std::string_view, int>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], data0[i], data1[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
      EXPECT_EQ(std::get<3>(input[i]), std::get<3>(output[i]));
    }
  }
  {
    std::vector<int64_t> srcs, dsts;
    this->OutputIncomingEndpoints(srcs, dsts, 0);
    ASSERT_EQ(srcs.size(), edge_num);
    ASSERT_EQ(dsts.size(), edge_num);
    std::vector<std::string_view> data0;
    this->OutputIncomingEdgeData<std::string_view>(data0, 0, 0);
    ASSERT_EQ(data0.size(), edge_num);
    std::vector<int> data1;
    this->OutputIncomingEdgeData<int>(data1, 0, 1);
    ASSERT_EQ(data1.size(), edge_num);
    std::vector<std::tuple<int64_t, int64_t, std::string_view, int>> output;
    for (size_t i = 0; i < edge_num; ++i) {
      output.emplace_back(srcs[i], dsts[i], data0[i], data1[i]);
    }
    std::sort(output.begin(), output.end());
    for (size_t i = 0; i < edge_num; ++i) {
      EXPECT_EQ(std::get<0>(input[i]), std::get<0>(output[i]));
      EXPECT_EQ(std::get<1>(input[i]), std::get<1>(output[i]));
      EXPECT_EQ(std::get<2>(input[i]), std::get<2>(output[i]));
      EXPECT_EQ(std::get<3>(input[i]), std::get<3>(output[i]));
    }
  }
}

TEST_F(EdgeTableTest, TestCountEdgeNum) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  int64_t src_num = 1000;
  int64_t dst_num = 1000;
  size_t edge_num = 20000;

  auto src_list = generate_random_vertices<int64_t>(src_num, edge_num);
  auto dst_list = generate_random_vertices<int64_t>(dst_num, edge_num);

  auto data_list = generate_random_data<int>(edge_num);

  auto src_arrs = convert_to_arrow_arrays(src_list, 10);
  auto dst_arrs = convert_to_arrow_arrays(dst_list, 10);
  auto data_arrs = convert_to_arrow_arrays(data_list, 10);

  auto batches = convert_to_record_batches({"src", "dst", "data"},
                                           {src_arrs, dst_arrs, data_arrs});

  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_int_);
  this->OpenEdgeTable();
  this->BatchInsert(std::move(batches));

  EXPECT_EQ(this->edge_table->EdgeNum(), edge_num);
}