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

#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/allocators.h"
#include "utils.h"

namespace gs {
namespace test {

class EdgeTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("edge_table_" + std::to_string(::getpid()) + "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    if (std::filesystem::exists(allocator_dir_)) {
      std::filesystem::remove_all(allocator_dir_);
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
    allocator_dir_ =
        "/tmp/edge_table_test_allocator_" + std::to_string(::getpid()) + "_";
  }
  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    if (std::filesystem::exists(allocator_dir_)) {
      std::filesystem::remove_all(allocator_dir_);
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

  gs::vid_t GetSrcLid(const gs::Property& src_oid) {
    gs::vid_t src_lid;
    if (!src_indexer.get_index(src_oid, src_lid)) {
      LOG(FATAL) << "Cannot find src oid " << src_oid.to_string();
    }
    return src_lid;
  }

  gs::vid_t GetDstLid(const gs::Property& dst_oid) {
    gs::vid_t dst_lid;
    if (!dst_indexer.get_index(dst_oid, dst_lid)) {
      LOG(FATAL) << "Cannot find dst oid " << dst_oid.to_string();
    }
    return dst_lid;
  }

  std::unique_ptr<gs::EdgeTable> edge_table = nullptr;
  gs::LFIndexer<gs::vid_t> src_indexer;
  gs::LFIndexer<gs::vid_t> dst_indexer;
  gs::Schema schema_;
  gs::label_t src_label_, dst_label_, edge_label_int_, edge_label_str_,
      edge_label_str_int_;
  std::string allocator_dir_;

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

TEST_F(EdgeTableTest, TestDeleteEdge) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  int64_t src_num = 100;
  int64_t dst_num = 100;
  size_t edge_num = 1000;

  // auto src_list = generate_random_vertices<int64_t>(src_num, edge_num);
  // auto dst_list = generate_random_vertices<int64_t>(dst_num, edge_num);
  std::vector<int64_t> src_list, dst_list;
  for (size_t i = 0; i < edge_num; ++i) {
    src_list.push_back(i % src_num);
    dst_list.push_back((i + 1 + (i / dst_num)) % dst_num);
  }

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

  size_t delete_count = 0;
  for (size_t i = 0; i < edge_num; ++i) {
    if (i % 10 == 0) {
      gs::vid_t src_lid = GetSrcLid(gs::Property::from_int64(src_list[i]));
      gs::vid_t dst_lid = GetDstLid(gs::Property::from_int64(dst_list[i]));
      this->edge_table->RemoveEdge(src_lid, dst_lid, gs::MAX_TIMESTAMP);
      delete_count++;
    }
  }

  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, gs::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num - delete_count);
  ASSERT_EQ(dsts.size(), edge_num - delete_count);
}

TEST_F(EdgeTableTest, TestBatchAddEdgesBundled) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 100;
  int64_t dst_num = 100;
  size_t edge_num = 1000;

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
  this->OpenEdgeTableInMemory(src_num, dst_num);
  this->BatchInsert(std::move(batches));
  EXPECT_EQ(this->edge_table->EdgeNum(), edge_num);

  // Generate more edges
  int64_t more_edge_num = 50;
  auto more_src_list = generate_random_vertices<gs::vid_t>(
      this->src_indexer.size(), more_edge_num);
  auto more_dst_list = generate_random_vertices<gs::vid_t>(
      this->dst_indexer.size(), more_edge_num);
  auto more_data_list = generate_random_data<int>(more_edge_num);
  std::vector<std::vector<gs::Property>> edge_data;
  for (size_t i = 0; i < more_edge_num; ++i) {
    edge_data.push_back({gs::Property::from_int32(more_data_list[i])});
  }

  // Insert more edges

  this->edge_table->BatchAddEdges(more_src_list, more_dst_list, edge_data);
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, gs::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num + more_edge_num);
  ASSERT_EQ(dsts.size(), edge_num + more_edge_num);
}

TEST_F(EdgeTableTest, TestBatchAddEdgesUnbundled) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 100;
  int64_t dst_num = 100;
  size_t edge_num = 1000;

  auto src_list = generate_random_vertices<int64_t>(src_num, edge_num);
  auto dst_list = generate_random_vertices<int64_t>(dst_num, edge_num);
  auto data0_list = generate_random_data<std::string>(edge_num);
  auto data1_list = generate_random_data<int>(edge_num);
  auto src_arrs = convert_to_arrow_arrays(src_list, 10);
  auto dst_arrs = convert_to_arrow_arrays(dst_list, 10);
  auto data0_arrs = convert_to_arrow_arrays(data0_list, 10);
  auto data1_arrs = convert_to_arrow_arrays(data1_list, 10);
  auto batches =
      convert_to_record_batches({"src", "dst", "data0", "data1"},
                                {src_arrs, dst_arrs, data0_arrs, data1_arrs});
  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTableInMemory(src_num, dst_num);
  this->BatchInsert(std::move(batches));
  EXPECT_EQ(this->edge_table->EdgeNum(), edge_num);

  // Generate more edges
  int64_t more_edge_num = 50;
  auto more_src_list = generate_random_vertices<gs::vid_t>(
      this->src_indexer.size(), more_edge_num);
  auto more_dst_list = generate_random_vertices<gs::vid_t>(
      this->dst_indexer.size(), more_edge_num);
  auto more_data_list0 = generate_random_data<std::string>(more_edge_num);
  auto more_data_list1 = generate_random_data<int>(more_edge_num);
  std::vector<std::vector<gs::Property>> edge_data;
  for (size_t i = 0; i < more_edge_num; ++i) {
    edge_data.push_back({gs::Property::from_string_view(more_data_list0[i]),
                         gs::Property::from_int32(more_data_list1[i])});
  }

  // Insert more edges

  this->edge_table->BatchAddEdges(more_src_list, more_dst_list, edge_data);
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, gs::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num + more_edge_num);
  ASSERT_EQ(dsts.size(), edge_num + more_edge_num);
}

TEST_F(EdgeTableTest, TestAddEdge) {
  // Test add with AddEdge()
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 10;
  int64_t dst_num = 10;
  int64_t edge_num = 100;
  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_int_);
  this->OpenEdgeTableInMemory(src_num, dst_num);
  std::vector<gs::vid_t> src_lids, dst_lids;
  std::vector<int64_t> src_oids =
      generate_random_vertices<int64_t>(src_num, edge_num);
  std::vector<int64_t> dst_oids =
      generate_random_vertices<int64_t>(dst_num, edge_num);
  for (auto src_oid : src_oids) {
    gs::vid_t src_lid =
        this->src_indexer.insert_safe(gs::Property::from_int64(src_oid));
    src_lids.push_back(src_lid);
  }
  for (auto dst_oid : dst_oids) {
    gs::vid_t dst_lid =
        this->dst_indexer.insert_safe(gs::Property::from_int64(dst_oid));
    dst_lids.push_back(dst_lid);
  }
  this->edge_table->Resize(this->src_indexer.size(), this->dst_indexer.size());
  std::vector<std::vector<gs::Property>> edge_data;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    edge_data.push_back({gs::Property::from_int32(static_cast<int>(i))});
  }

  gs::Allocator allocator(gs::MemoryStrategy::kMemoryOnly, allocator_dir_);

  size_t edge_count = 0;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    this->edge_table->AddEdge(src_lids[i], dst_lids[i], edge_data[i],
                              gs::MAX_TIMESTAMP, allocator);
    edge_count++;
  }
  EXPECT_EQ(edge_count, src_lids.size());
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, gs::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num);
  ASSERT_EQ(dsts.size(), edge_num);
}
template <typename EDATA_T, typename ARROW_COL_T>
struct TypePair {
  using EdType = EDATA_T;
  using ArrowType = ARROW_COL_T;
};
using Datatypes =
    ::testing::Types<TypePair<int32_t, arrow::Int32Array>,
                     TypePair<uint32_t, arrow::UInt32Array>,
                     TypePair<int64_t, arrow::Int64Array>,
                     TypePair<uint64_t, arrow::UInt64Array>,
                     TypePair<float, arrow::FloatArray>,
                     TypePair<double, arrow::DoubleArray>,
                     TypePair<gs::Date, arrow::Date32Array>,
                     TypePair<gs::DateTime, arrow::TimestampArray>,
                     TypePair<gs::Interval, arrow::LargeStringArray>>;

template <typename T>
class EdgeTableToolsTest : public ::testing::Test {};
TYPED_TEST_SUITE(EdgeTableToolsTest, Datatypes);

TYPED_TEST(EdgeTableToolsTest, TestBatchAddEdges) {
  using EdType = typename TypeParam::EdType;
  const char* var = std::getenv("STORAGE_RESOURCE");
  std::string resource_path =
      var ? var : "/workspaces/neug/tests/storage/resources";
  std::shared_ptr<EdgeSchema> edge_schema = std::make_shared<EdgeSchema>();
  edge_schema->ie_mutable = true;
  edge_schema->oe_mutable = true;
  edge_schema->ie_strategy = EdgeStrategy::kMultiple;
  edge_schema->oe_strategy = EdgeStrategy::kMultiple;
  std::vector<std::string> property_name = {"test_property"};
  std::vector<StorageStrategy> storage_strategy = {StorageStrategy::kMem};

  std::string file_path;
  std::vector<PropertyType> column_types = {PropertyType::kUInt32,
                                            PropertyType::kUInt32};
  std::unordered_map<std::string, std::string> csv_options;
  csv_options.insert({"HEADER", "FALSE"});
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  if constexpr (std::is_same_v<EdType, int32_t>) {
    file_path = resource_path + "/edges_i32.csv";
    std::vector<PropertyType> property_type = {PropertyType::kInt32};
    column_types.emplace_back(PropertyType::kInt32);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, int64_t>) {
    file_path = resource_path + "/edges_i64.csv";
    std::vector<PropertyType> property_type = {PropertyType::kInt64};
    column_types.emplace_back(PropertyType::kInt64);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, uint32_t>) {
    file_path = resource_path + "/edges_u32.csv";
    std::vector<PropertyType> property_type = {PropertyType::kUInt32};
    column_types.emplace_back(PropertyType::kUInt32);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, uint64_t>) {
    file_path = resource_path + "/edges_u64.csv";
    std::vector<PropertyType> property_type = {PropertyType::kUInt64};
    column_types.emplace_back(PropertyType::kUInt64);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, float>) {
    file_path = resource_path + "/edges_float.csv";
    std::vector<PropertyType> property_type = {PropertyType::kFloat};
    column_types.emplace_back(PropertyType::kFloat);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, double>) {
    file_path = resource_path + "/edges_double.csv";
    std::vector<PropertyType> property_type = {PropertyType::kDouble};
    column_types.emplace_back(PropertyType::kDouble);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, Date>) {
    file_path = resource_path + "/edges_date.csv";
    std::vector<PropertyType> property_type = {PropertyType::kDate};
    column_types.emplace_back(PropertyType::kDate);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, DateTime>) {
    file_path = resource_path + "/edges_datetime.csv";
    std::vector<PropertyType> property_type = {PropertyType::kDateTime};
    column_types.emplace_back(PropertyType::kDateTime);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, Interval>) {
    file_path = resource_path + "/edges_interval.csv";
    std::vector<PropertyType> property_type = {PropertyType::kInterval};
    column_types.emplace_back(PropertyType::kInterval);
    edge_schema->add_properties(property_name, property_type, storage_strategy);
    suppliers = runtime::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else {
    FAIL();
  }
  EXPECT_EQ(suppliers.size(), 1);

  LFIndexer<vid_t> indexer;
  indexer.init(PropertyType::kUInt32);
  indexer.reserve(10);
  for (uint32_t i = 0; i < 10; i++) {
    Property oid;
    oid.set_uint32(i);
    indexer.insert(oid);
  }

  EdgeTable e_table = EdgeTable(edge_schema);
  e_table.BatchAddEdges(indexer, indexer, suppliers[0]);
  EXPECT_EQ(e_table.EdgeNum(), 10);

  std::vector<std::string> new_property_name = {"new_property"};
  std::vector<PropertyType> new_property_type = {PropertyType::kInt32};
  edge_schema->add_properties(new_property_name, new_property_type);
  e_table.AddProperties(new_property_name, new_property_type);
  EXPECT_EQ(e_table.get_properties_table().col_num(), 2);
}

TYPED_TEST(EdgeTableToolsTest, TestAddProperties) {
  using EdType = typename TypeParam::EdType;
  const char* var = std::getenv("STORAGE_RESOURCE");
  std::string resource_path =
      var ? var : "/workspaces/neug/tests/storage/resources";
  std::shared_ptr<EdgeSchema> edge_schema = std::make_shared<EdgeSchema>();
  edge_schema->ie_mutable = true;
  edge_schema->oe_mutable = true;
  edge_schema->ie_strategy = EdgeStrategy::kMultiple;
  edge_schema->oe_strategy = EdgeStrategy::kMultiple;
  std::vector<StorageStrategy> storage_strategy = {StorageStrategy::kMem};

  std::string file_path = resource_path + "/edges_empty.csv";
  std::vector<PropertyType> column_types = {PropertyType::kUInt32,
                                            PropertyType::kUInt32};
  std::unordered_map<std::string, std::string> csv_options;
  csv_options.insert({"HEADER", "FALSE"});
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  suppliers = runtime::ops::create_csv_record_suppliers(file_path, column_types,
                                                        csv_options);
  EXPECT_EQ(suppliers.size(), 1);

  LFIndexer<vid_t> indexer;
  indexer.init(PropertyType::kUInt32);
  indexer.reserve(10);
  for (uint32_t i = 0; i < 10; i++) {
    Property oid;
    oid.set_uint32(i);
    indexer.insert(oid);
  }

  std::vector<std::string> new_property_name = {"new_property"};
  std::vector<PropertyType> new_property_type;
  EdgeTable e_table = EdgeTable(edge_schema);
  e_table.BatchAddEdges(indexer, indexer, suppliers[0]);
  EXPECT_EQ(e_table.EdgeNum(), 10);
  if constexpr (std::is_same_v<EdType, int32_t>) {
    new_property_type = {PropertyType::kInt32};
  } else if constexpr (std::is_same_v<EdType, int64_t>) {
    new_property_type = {PropertyType::kInt64};
  } else if constexpr (std::is_same_v<EdType, uint32_t>) {
    new_property_type = {PropertyType::kUInt32};
  } else if constexpr (std::is_same_v<EdType, uint64_t>) {
    new_property_type = {PropertyType::kUInt64};
  } else if constexpr (std::is_same_v<EdType, float>) {
    new_property_type = {PropertyType::kFloat};
  } else if constexpr (std::is_same_v<EdType, double>) {
    new_property_type = {PropertyType::kDouble};
  } else if constexpr (std::is_same_v<EdType, Date>) {
    new_property_type = {PropertyType::kDate};
  } else if constexpr (std::is_same_v<EdType, DateTime>) {
    new_property_type = {PropertyType::kDateTime};
  } else if constexpr (std::is_same_v<EdType, Interval>) {
    new_property_type = {PropertyType::kInterval};
  } else {
    FAIL();
  }

  edge_schema->add_properties(new_property_name, new_property_type);
  e_table.AddProperties(new_property_name, new_property_type);
}

}  // namespace test
}  // namespace gs
