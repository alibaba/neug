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
#include "neug/storages/allocators.h"
#include "neug/storages/csr/generic_view_utils.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/loader/loader_utils.h"
#include "unittest/utils.h"

namespace neug {
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
                           {std::make_tuple(neug::DataTypeId::kInt64, "id", 0)},

                           static_cast<size_t>(1) << 32, "person vertex label");
    schema_.AddVertexLabel(
        "comment", {}, {}, {std::make_tuple(neug::DataTypeId::kInt64, "id", 0)},
        static_cast<size_t>(1) << 32, "comment vertex label");
    schema_.AddEdgeLabel("person", "comment", "create0", {}, {},
                         neug::EdgeStrategy::kMultiple,
                         neug::EdgeStrategy::kMultiple, true, true, false,
                         "person creates comment edge without properties");
    schema_.AddEdgeLabel(
        "person", "comment", "create1", {neug::DataTypeId::kInt32}, {"data"},
        neug::EdgeStrategy::kMultiple, neug::EdgeStrategy::kMultiple, true,
        true, false, "person creates comment edge");
    schema_.AddEdgeLabel(
        "person", "comment", "create2", {neug::DataTypeId::kVarchar}, {"data"},
        neug::EdgeStrategy::kMultiple, neug::EdgeStrategy::kMultiple, true,
        true, false, "person creates comment edge");
    schema_.AddEdgeLabel("person", "comment", "create3",
                         {neug::DataTypeId::kVarchar, neug::DataTypeId::kInt32},
                         {"data0", "data1"}, neug::EdgeStrategy::kMultiple,
                         neug::EdgeStrategy::kMultiple, true, true, false,
                         "person creates comment edge with two properties");
    src_label_ = schema_.get_vertex_label_id("person");
    dst_label_ = schema_.get_vertex_label_id("comment");
    edge_label_empty_ = schema_.get_edge_label_id("create0");
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

  void build_indexer(neug::LFIndexer<neug::vid_t>& indexer, neug::vid_t num,
                     const std::string& name, const std::string& snapshot_dir,
                     const std::string& work_dir) {
    indexer.drop();
    indexer.init(DataTypeId::kInt64);
    indexer.open(name, snapshot_dir, work_dir);
    indexer.reserve(num);
    for (neug::vid_t i = 0; i < num; ++i) {
      indexer.insert(PropUtils<int64_t>::to_prop(i), i);
    }
  }

  void InitIndexers(neug::vid_t src_num, neug::vid_t dst_num) {
    build_indexer(src_indexer, src_num, "src_indexer",
                  SnapshotDirectory().string(), WorkDirectory().string());
    build_indexer(dst_indexer, dst_num, "dst_indexer",
                  SnapshotDirectory().string(), WorkDirectory().string());
  }

  void ConstructEdgeTable(neug::label_t src_label, neug::label_t dst_label,
                          neug::label_t edge_label) {
    edge_table = std::make_unique<neug::EdgeTable>(
        schema_.get_edge_schema(src_label, dst_label, edge_label));
  }

  void OpenEdgeTable() {
    edge_table->Open(WorkDirectory().string(), MemoryLevel::kSyncToFile);
  }

  void OpenEdgeTableInMemory(size_t src_v_cap, size_t dst_v_cap) {
    edge_table->Open(SnapshotDirectory().string(), MemoryLevel::kInMemory);
    edge_table->EnsureCapacity(src_v_cap, dst_v_cap);
  }

  void BatchInsert(std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) {
    auto supplier =
        std::make_shared<GeneratedRecordBatchSupplier>(std::move(batches));
    edge_table->BatchAddEdges(src_indexer, dst_indexer, supplier);
  }

  size_t ExpectedBatchInsertCapacity(size_t inserted_edge_num) const {
    if (inserted_edge_num == 0) {
      return 0;
    }
    size_t new_cap = inserted_edge_num;
    while (inserted_edge_num >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    return new_cap;
  }

  void ExpectBundledStats(size_t expected_size) const {
    ASSERT_NE(edge_table, nullptr);
    EXPECT_EQ(edge_table->PropTableSize(), 0);
    EXPECT_EQ(edge_table->Capacity(), neug::CsrBase::INFINITE_CAPACITY);
  }

  void ExpectUnbundledStats(size_t expected_size,
                            size_t expected_capacity) const {
    ASSERT_NE(edge_table, nullptr);
    EXPECT_EQ(edge_table->PropTableSize(), expected_size);
    EXPECT_EQ(edge_table->Capacity(), expected_capacity);
  }

  void OutputOutgoingEndpoints(std::vector<int64_t>& srcs,
                               std::vector<int64_t>& dsts,
                               neug::timestamp_t ts) {
    auto view = edge_table->get_outgoing_view(ts);
    neug::vid_t vnum = src_indexer.size();
    for (neug::vid_t i = 0; i < vnum; ++i) {
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
                               std::vector<int64_t>& dsts,
                               neug::timestamp_t ts) {
    auto view = edge_table->get_incoming_view(ts);
    neug::vid_t vnum = dst_indexer.size();
    for (neug::vid_t i = 0; i < vnum; ++i) {
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
  void OutputOutgoingEdgeData(std::vector<T>& data, neug::timestamp_t ts,
                              int prop_id) {
    auto ed_accessor = edge_table->get_edge_data_accessor(prop_id);
    auto view = edge_table->get_outgoing_view(ts);
    neug::vid_t vnum = src_indexer.size();
    for (neug::vid_t i = 0; i < vnum; ++i) {
      auto es = view.get_edges(i);
      for (auto it = es.begin(); it != es.end(); ++it) {
        data.push_back(ed_accessor.get_typed_data<T>(it));
      }
    }
  }

  template <typename T>
  void OutputIncomingEdgeData(std::vector<T>& data, neug::timestamp_t ts,
                              int prop_id) {
    auto ed_accessor = edge_table->get_edge_data_accessor(prop_id);
    auto view = edge_table->get_incoming_view(ts);
    neug::vid_t vnum = dst_indexer.size();
    for (neug::vid_t i = 0; i < vnum; ++i) {
      auto es = view.get_edges(i);
      for (auto it = es.begin(); it != es.end(); ++it) {
        data.push_back(ed_accessor.get_typed_data<T>(it));
      }
    }
  }

  neug::vid_t GetSrcLid(const neug::Property& src_oid) {
    neug::vid_t src_lid;
    if (!src_indexer.get_index(src_oid, src_lid)) {
      LOG(FATAL) << "Cannot find src oid " << src_oid.to_string();
    }
    return src_lid;
  }

  neug::vid_t GetDstLid(const neug::Property& dst_oid) {
    neug::vid_t dst_lid;
    if (!dst_indexer.get_index(dst_oid, dst_lid)) {
      LOG(FATAL) << "Cannot find dst oid " << dst_oid.to_string();
    }
    return dst_lid;
  }

  std::unique_ptr<neug::EdgeTable> edge_table = nullptr;
  neug::LFIndexer<neug::vid_t> src_indexer;
  neug::LFIndexer<neug::vid_t> dst_indexer;
  neug::Schema schema_;
  neug::label_t src_label_, dst_label_, edge_label_empty_, edge_label_int_,
      edge_label_str_, edge_label_str_int_;
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
  this->ExpectBundledStats(0);
  this->BatchInsert(std::move(batches));
  this->ExpectBundledStats(edge_num);

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
  this->ExpectUnbundledStats(0, 0);
  this->BatchInsert(std::move(batches));
  this->ExpectUnbundledStats(edge_num, ExpectedBatchInsertCapacity(edge_num));

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
  this->ExpectUnbundledStats(0, 0);
  this->BatchInsert(std::move(batches));
  this->ExpectUnbundledStats(edge_num, ExpectedBatchInsertCapacity(edge_num));

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
  this->ExpectUnbundledStats(edge_num, ExpectedBatchInsertCapacity(edge_num));
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
  this->ExpectBundledStats(edge_num);
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
  this->ExpectBundledStats(edge_num);
  auto oe_view = this->edge_table->get_outgoing_view(neug::MAX_TIMESTAMP);
  auto ie_view = this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);

  size_t delete_count = 0;
  for (size_t i = 0; i < edge_num; ++i) {
    if (i % 10 == 0) {
      neug::vid_t src_lid = GetSrcLid(neug::Property::from_int64(src_list[i]));
      neug::vid_t dst_lid = GetDstLid(neug::Property::from_int64(dst_list[i]));
      auto es = oe_view.get_edges(src_lid);
      auto is = ie_view.get_edges(dst_lid);
      for (auto it = es.begin(); it != es.end(); ++it) {
        if (it.get_vertex() == dst_lid) {
          auto another_offset = neug::fuzzy_search_offset_from_nbr_list(
              is, src_lid, it.get_data_ptr(), DataTypeId::kInt32);
          auto oe_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                            reinterpret_cast<const char*>(es.start_ptr)) /
                           es.cfg.stride;
          this->edge_table->DeleteEdge(src_lid, dst_lid, oe_offset,
                                       another_offset, neug::MAX_TIMESTAMP);
          delete_count++;
        }
      }
    }
  }

  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num - delete_count);
  ASSERT_EQ(dsts.size(), edge_num - delete_count);
  this->ExpectBundledStats(edge_num - delete_count);

  // Test delete edge with soft delete, and revert.
  size_t soft_delete_todo = std::max(100, (int32_t) srcs.size() / 10);
  std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>> soft_deleted_edges;
  for (neug::vid_t src_lid = 0; src_lid < this->src_indexer.size() &&
                                soft_deleted_edges.size() < soft_delete_todo;
       ++src_lid) {
    auto es = oe_view.get_edges(src_lid);
    for (auto it = es.begin(); it != es.end(); ++it) {
      auto dst_lid = it.get_vertex();
      auto is = ie_view.get_edges(dst_lid);
      auto another_offset = neug::fuzzy_search_offset_from_nbr_list(
          is, src_lid, it.get_data_ptr(), DataTypeId::kInt32);
      auto oe_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                        reinterpret_cast<const char*>(es.start_ptr)) /
                       es.cfg.stride;
      this->edge_table->DeleteEdge(src_lid, dst_lid, oe_offset, another_offset,
                                   neug::MAX_TIMESTAMP);
      soft_deleted_edges.emplace_back(src_lid, dst_lid, oe_offset,
                                      another_offset);
      break;
    }
  }

  {
    std::vector<int64_t> tmp_srcs, tmp_dsts;
    this->OutputOutgoingEndpoints(tmp_srcs, tmp_dsts, neug::MAX_TIMESTAMP);
    ASSERT_EQ(tmp_srcs.size(),
              edge_num - delete_count - soft_deleted_edges.size());
    ASSERT_EQ(tmp_dsts.size(),
              edge_num - delete_count - soft_deleted_edges.size());
    this->ExpectBundledStats(edge_num - delete_count -
                             soft_deleted_edges.size());
  }
  // Revert soft deleted edges
  for (const auto& edge_record : soft_deleted_edges) {
    this->edge_table->RevertDeleteEdge(
        std::get<0>(edge_record), std::get<1>(edge_record),
        std::get<2>(edge_record), std::get<3>(edge_record), 0);
  }
  {
    std::vector<int64_t> tmp_srcs, tmp_dsts;
    this->OutputOutgoingEndpoints(tmp_srcs, tmp_dsts, neug::MAX_TIMESTAMP);
    ASSERT_EQ(tmp_srcs.size(), edge_num - delete_count);
    ASSERT_EQ(tmp_dsts.size(), edge_num - delete_count);
    this->ExpectBundledStats(edge_num - delete_count);
  }
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
  this->ExpectBundledStats(0);
  this->BatchInsert(std::move(batches));
  EXPECT_EQ(this->edge_table->EdgeNum(), edge_num);
  this->ExpectBundledStats(edge_num);

  // Generate more edges
  int64_t more_edge_num = 50;
  auto more_src_list = generate_random_vertices<neug::vid_t>(
      this->src_indexer.size(), more_edge_num);
  auto more_dst_list = generate_random_vertices<neug::vid_t>(
      this->dst_indexer.size(), more_edge_num);
  auto more_data_list = generate_random_data<int>(more_edge_num);
  std::vector<std::vector<neug::Property>> edge_data;
  for (size_t i = 0; i < more_edge_num; ++i) {
    edge_data.push_back({neug::Property::from_int32(more_data_list[i])});
  }

  // Insert more edges

  this->edge_table->BatchAddEdges(more_src_list, more_dst_list, edge_data);
  this->ExpectBundledStats(edge_num + more_edge_num);
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
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
  this->ExpectUnbundledStats(0, 0);
  this->BatchInsert(std::move(batches));
  EXPECT_EQ(this->edge_table->EdgeNum(), edge_num);
  this->ExpectUnbundledStats(edge_num, ExpectedBatchInsertCapacity(edge_num));

  // Generate more edges
  int64_t more_edge_num = 50;
  auto more_src_list = generate_random_vertices<neug::vid_t>(
      this->src_indexer.size(), more_edge_num);
  auto more_dst_list = generate_random_vertices<neug::vid_t>(
      this->dst_indexer.size(), more_edge_num);
  auto more_data_list0 = generate_random_data<std::string>(more_edge_num);
  auto more_data_list1 = generate_random_data<int>(more_edge_num);
  std::vector<std::vector<neug::Property>> edge_data;
  for (size_t i = 0; i < more_edge_num; ++i) {
    edge_data.push_back({neug::Property::from_string_view(more_data_list0[i]),
                         neug::Property::from_int32(more_data_list1[i])});
  }

  // Insert more edges

  this->edge_table->BatchAddEdges(more_src_list, more_dst_list, edge_data);
  this->ExpectUnbundledStats(edge_num + more_edge_num,
                             ExpectedBatchInsertCapacity(edge_num));
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num + more_edge_num);
  ASSERT_EQ(dsts.size(), edge_num + more_edge_num);
}

TEST_F(EdgeTableTest, TestAddEdgeAndDelete) {
  // Test add with AddEdge()
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 10;
  int64_t dst_num = 10;
  int64_t edge_num = 100;
  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_int_);
  this->OpenEdgeTableInMemory(src_num, dst_num);
  std::vector<neug::vid_t> src_lids, dst_lids;
  std::vector<int64_t> src_oids =
      generate_random_vertices<int64_t>(src_num, edge_num);
  std::vector<int64_t> dst_oids =
      generate_random_vertices<int64_t>(dst_num, edge_num);
  for (auto src_oid : src_oids) {
    neug::vid_t src_lid =
        this->src_indexer.insert(neug::Property::from_int64(src_oid), true);
    src_lids.push_back(src_lid);
  }
  for (auto dst_oid : dst_oids) {
    neug::vid_t dst_lid =
        this->dst_indexer.insert(neug::Property::from_int64(dst_oid), true);
    dst_lids.push_back(dst_lid);
  }
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size());
  this->ExpectBundledStats(0);
  std::vector<std::vector<neug::Property>> edge_data;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    edge_data.push_back({neug::Property::from_int32(static_cast<int>(i))});
  }

  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);

  size_t edge_count = 0;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    this->edge_table->AddEdge(src_lids[i], dst_lids[i], edge_data[i], 0,
                              allocator, false);
    edge_count++;
  }
  EXPECT_EQ(edge_count, src_lids.size());
  this->ExpectBundledStats(edge_num);
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num);
  ASSERT_EQ(dsts.size(), edge_num);

  auto oe_view = this->edge_table->get_outgoing_view(neug::MAX_TIMESTAMP);
  auto ie_view = this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>> edges_to_delete;
  for (neug::vid_t vid = 0; vid < this->src_indexer.size(); ++vid) {
    auto es = oe_view.get_edges(vid);
    for (auto it = es.begin(); it != es.end(); ++it) {
      if (it.get_vertex() % 2 == 0) {
        auto oe_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                          reinterpret_cast<const char*>(es.start_ptr)) /
                         es.cfg.stride;
        auto is = ie_view.get_edges(it.get_vertex());
        auto another_offset = neug::fuzzy_search_offset_from_nbr_list(
            is, vid, it.get_data_ptr(), DataTypeId::kInt32);
        edges_to_delete.emplace_back(
            std::make_tuple(vid, it.get_vertex(), oe_offset, another_offset));
        EXPECT_NE(oe_offset, std::numeric_limits<int32_t>::max());
        EXPECT_NE(another_offset, std::numeric_limits<int32_t>::max());
        this->edge_table->DeleteEdge(vid, it.get_vertex(), oe_offset,
                                     another_offset, neug::MAX_TIMESTAMP);
      }
    }
  }

  srcs.clear();
  dsts.clear();
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num - edges_to_delete.size());
  ASSERT_EQ(dsts.size(), edge_num - edges_to_delete.size());
  this->ExpectBundledStats(edge_num - edges_to_delete.size());
  srcs.clear();
  dsts.clear();
  this->OutputIncomingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num - edges_to_delete.size());
  ASSERT_EQ(dsts.size(), edge_num - edges_to_delete.size());

  // Revert deleted edges
  for (const auto& edge_record : edges_to_delete) {
    this->edge_table->RevertDeleteEdge(
        std::get<0>(edge_record), std::get<1>(edge_record),
        std::get<2>(edge_record), std::get<3>(edge_record), 0);
  }
  srcs.clear();
  dsts.clear();
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num);
  ASSERT_EQ(dsts.size(), edge_num);
  this->ExpectBundledStats(edge_num);
  srcs.clear();
  dsts.clear();
  this->OutputIncomingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num);
  ASSERT_EQ(dsts.size(), edge_num);

  // Test Delete multiple same edges with different timestamp.
  for (timestamp_t ts = 1; ts < 10; ++ts) {
    this->edge_table->AddEdge(0, 1, edge_data[0], ts, allocator, false);
  }
  this->ExpectBundledStats(edge_num + 9);
  std::vector<
      std::pair<std::tuple<vid_t, vid_t, int32_t, int32_t>, timestamp_t>>
      multi_edges_to_delete;
  oe_view = this->edge_table->get_outgoing_view(neug::MAX_TIMESTAMP);
  ie_view = this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  auto oes = oe_view.get_edges(0);
  auto ies = ie_view.get_edges(1);
  for (auto it = ies.begin(); it != ies.end(); ++it) {
    if (it.get_vertex() == 0 && it.get_timestamp() % 2 == 1) {
      auto ie_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                        reinterpret_cast<const char*>(ies.start_ptr)) /
                       ies.cfg.stride;
      auto oe_offset = neug::fuzzy_search_offset_from_nbr_list(
          oes, 1, it.get_data_ptr(), DataTypeId::kInt32);
      EXPECT_NE(oe_offset, std::numeric_limits<int32_t>::max());
      EXPECT_NE(ie_offset, std::numeric_limits<int32_t>::max());
      multi_edges_to_delete.emplace_back(
          std::make_tuple(0, 1, oe_offset, ie_offset), it.get_timestamp());
      this->edge_table->DeleteEdge(0, 1, oe_offset, ie_offset,
                                   it.get_timestamp());
    }
  }
  auto view_after_delete =
      this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  auto es_after_delete = view_after_delete.get_edges(1);
  for (auto it = es_after_delete.begin(); it != es_after_delete.end(); ++it) {
    EXPECT_FALSE(it.get_vertex() == 0 && it.get_timestamp() % 2 == 1);
  }
  this->ExpectBundledStats(edge_num + 9 - multi_edges_to_delete.size());
  for (const auto& pair : multi_edges_to_delete) {
    const auto& edge_record = pair.first;
    this->edge_table->RevertDeleteEdge(
        std::get<0>(edge_record), std::get<1>(edge_record),
        std::get<2>(edge_record), std::get<3>(edge_record), pair.second);
  }
  auto view_after_revert =
      this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  auto es_after_revert = view_after_revert.get_edges(1);
  size_t revert_count = 0;
  for (auto it = es_after_revert.begin(); it != es_after_revert.end(); ++it) {
    if (it.get_vertex() == 0 && it.get_timestamp() % 2 == 1) {
      revert_count++;
    }
  }
  EXPECT_EQ(revert_count, multi_edges_to_delete.size());
  this->ExpectBundledStats(edge_num + 9);
}

TEST_F(EdgeTableTest, TestAddEdgeDeleteUnbundled) {
  // Test add with AddEdge()
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 10;
  int64_t dst_num = 10;
  int64_t edge_num = 100;
  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTableInMemory(src_num, dst_num);
  std::vector<neug::vid_t> src_lids, dst_lids;
  std::vector<int64_t> src_oids =
      generate_random_vertices<int64_t>(src_num, edge_num);
  std::vector<int64_t> dst_oids =
      generate_random_vertices<int64_t>(dst_num, edge_num);
  for (auto src_oid : src_oids) {
    neug::vid_t src_lid =
        this->src_indexer.insert(neug::Property::from_int64(src_oid), true);
    src_lids.push_back(src_lid);
  }
  for (auto dst_oid : dst_oids) {
    neug::vid_t dst_lid =
        this->dst_indexer.insert(neug::Property::from_int64(dst_oid), true);
    dst_lids.push_back(dst_lid);
  }
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size());
  this->ExpectUnbundledStats(0, 0);
  std::vector<std::vector<neug::Property>> edge_data;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    edge_data.push_back({neug::Property::from_string_view("edge_data"),
                         neug::Property::from_int32(static_cast<int>(i))});
  }

  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);

  size_t edge_count = 0;
  this->edge_table->EnsureCapacity(edge_data.size());
  this->ExpectUnbundledStats(0, 4096);
  for (size_t i = 0; i < src_lids.size(); ++i) {
    this->edge_table->AddEdge(src_lids[i], dst_lids[i], edge_data[i], 0,
                              allocator, false);
    edge_count++;
  }
  EXPECT_EQ(edge_count, src_lids.size());
  this->ExpectUnbundledStats(edge_num, 4096);
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), edge_num);
  ASSERT_EQ(dsts.size(), edge_num);

  auto oe_view = this->edge_table->get_outgoing_view(neug::MAX_TIMESTAMP);
  auto ie_view = this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  std::vector<size_t> deleted_edge_indices;
  size_t cur_index = 0;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    auto es = oe_view.get_edges(src_lids[i]);
    for (auto it = es.begin(); it != es.end(); ++it) {
      if (it.get_vertex() == dst_lids[i] && (cur_index % 10 == 0)) {
        auto is = ie_view.get_edges(dst_lids[i]);
        auto oe_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                          reinterpret_cast<const char*>(es.start_ptr)) /
                         es.cfg.stride;
        auto another_offset = neug::fuzzy_search_offset_from_nbr_list(
            is, src_lids[i], it.get_data_ptr(), DataTypeId::kUInt64);
        this->edge_table->DeleteEdge(src_lids[i], dst_lids[i], oe_offset,
                                     another_offset, neug::MAX_TIMESTAMP);
        deleted_edge_indices.push_back(cur_index);
      }
      cur_index++;
    }
  }
  std::vector<int64_t> srcs_after_delete, dsts_after_delete;
  this->OutputOutgoingEndpoints(srcs_after_delete, dsts_after_delete,
                                neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs_after_delete.size(), edge_num - deleted_edge_indices.size());
  ASSERT_EQ(dsts_after_delete.size(), edge_num - deleted_edge_indices.size());
  this->ExpectUnbundledStats(edge_num, 4096);
  for (size_t i = 0, j = 0; i < edge_num; ++i) {
    if (j < deleted_edge_indices.size() && i == deleted_edge_indices[j]) {
      j++;
      continue;
    }
    EXPECT_EQ(srcs[i], srcs_after_delete[i - j]);
    EXPECT_EQ(dsts[i], dsts_after_delete[i - j]);
  }
}

TEST_F(EdgeTableTest, TestEdgeTableCompaction) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 100;
  int64_t dst_num = 100;
  int64_t edge_num = 1000;
  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_int_);
  this->OpenEdgeTableInMemory(src_num, dst_num);
  std::vector<neug::vid_t> src_lids, dst_lids;
  std::vector<int64_t> src_oids =
      generate_random_vertices<int64_t>(src_num, edge_num);
  std::vector<int64_t> dst_oids =
      generate_random_vertices<int64_t>(dst_num, edge_num);
  for (auto src_oid : src_oids) {
    neug::vid_t src_lid =
        this->src_indexer.insert(neug::Property::from_int64(src_oid), true);
    src_lids.push_back(src_lid);
  }
  for (auto dst_oid : dst_oids) {
    neug::vid_t dst_lid =
        this->dst_indexer.insert(neug::Property::from_int64(dst_oid), true);
    dst_lids.push_back(dst_lid);
  }
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size());
  this->ExpectBundledStats(0);
  std::vector<std::vector<neug::Property>> edge_data;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    edge_data.push_back({neug::Property::from_int32(static_cast<int>(i))});
  }

  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);
  for (size_t i = 0; i < src_lids.size(); ++i) {
    this->edge_table->AddEdge(src_lids[i], dst_lids[i], edge_data[i], 0,
                              allocator, false);
  }
  this->ExpectBundledStats(edge_num);
  auto oe_view = this->edge_table->get_outgoing_view(neug::MAX_TIMESTAMP);
  auto ie_view = this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  size_t delete_count = 0;
  std::vector<std::tuple<neug::vid_t, neug::vid_t, int32_t, int32_t>>
      edges_to_delete;
  for (size_t i = 0; i < src_lids.size(); i += 3) {
    auto oe_edges = oe_view.get_edges(src_lids[i]);
    auto ie_edges = ie_view.get_edges(dst_lids[i]);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      auto oe_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                        reinterpret_cast<const char*>(oe_edges.start_ptr)) /
                       oe_edges.cfg.stride;
      auto ie_offset = neug::fuzzy_search_offset_from_nbr_list(
          ie_edges, src_lids[i], it.get_data_ptr(), DataTypeId::kInt32);
      if (ie_offset == std::numeric_limits<int32_t>::max()) {
        FAIL() << "Cannot find reverse edge!";
      }
      edges_to_delete.emplace_back(
          std::make_tuple(src_lids[i], dst_lids[i], oe_offset, ie_offset));
      this->edge_table->DeleteEdge(src_lids[i], dst_lids[i], oe_offset,
                                   ie_offset, 0);
      delete_count++;
    }
  }
  this->ExpectBundledStats(edge_num - delete_count);
  this->edge_table->Compact(true, false, neug::MAX_TIMESTAMP);
  this->ExpectBundledStats(edge_num - delete_count);
  size_t edge_count = 0;
  for (size_t i = 0; i < dst_lids.size(); ++i) {
    auto edges = ie_view.get_edges(dst_lids[i]);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      edge_count++;
    }
  }
  EXPECT_EQ(edge_count, edge_num - delete_count);
}

TEST_F(EdgeTableTest, TestUpdateEdgeData) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  int64_t src_num = 10;
  int64_t dst_num = 10;
  int64_t edge_num = 100;
  this->InitIndexers(src_num, dst_num);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTableInMemory(src_num, dst_num);
  std::vector<neug::vid_t> src_lids, dst_lids;
  std::vector<int64_t> src_oids =
      generate_random_vertices<int64_t>(src_num, edge_num);
  std::vector<int64_t> dst_oids =
      generate_random_vertices<int64_t>(dst_num, edge_num);
  for (auto src_oid : src_oids) {
    neug::vid_t src_lid =
        this->src_indexer.insert(neug::Property::from_int64(src_oid), true);
    src_lids.push_back(src_lid);
  }
  for (auto dst_oid : dst_oids) {
    neug::vid_t dst_lid =
        this->dst_indexer.insert(neug::Property::from_int64(dst_oid), true);
    dst_lids.push_back(dst_lid);
  }
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size());
  this->ExpectUnbundledStats(0, 0);
  std::vector<std::vector<neug::Property>> edge_data;
  for (size_t i = 0; i < src_lids.size(); ++i) {
    edge_data.push_back({neug::Property::from_string_view("old_data"),
                         neug::Property::from_int32(static_cast<int>(0))});
  }

  this->edge_table->EnsureCapacity(edge_data.size());
  this->ExpectUnbundledStats(0, 4096);
  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);
  for (size_t i = 0; i < src_lids.size(); ++i) {
    this->edge_table->AddEdge(src_lids[i], dst_lids[i], edge_data[i], 0,
                              allocator, false);
  }
  this->ExpectUnbundledStats(edge_num, 4096);
  std::vector<neug::Property> new_data = {
      neug::Property::from_string_view("new_data"),
      neug::Property::from_int32(static_cast<int>(1))};
  auto oe_view = this->edge_table->get_outgoing_view(neug::MAX_TIMESTAMP);
  auto ie_view = this->edge_table->get_incoming_view(neug::MAX_TIMESTAMP);
  auto ed_accessor_0 = this->edge_table->get_edge_data_accessor(0);
  auto ed_accessor_1 = this->edge_table->get_edge_data_accessor(1);
  for (size_t i = 0; i < src_lids.size(); ++i) {
    auto oe_edges = oe_view.get_edges(src_lids[i]);
    auto ie_edges = ie_view.get_edges(dst_lids[i]);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      assert(reinterpret_cast<const uint64_t*>(it.get_data_ptr()) != nullptr);
      auto another_offset = neug::fuzzy_search_offset_from_nbr_list(
          ie_edges, src_lids[i], it.get_data_ptr(), DataTypeId::kUInt64);
      auto another_iter = ie_edges.begin();
      another_iter += another_offset;
      if (another_iter.get_nbr_ptr() == nullptr) {
        FAIL() << "Cannot find reverse edge!";
      }
      ed_accessor_0.set_data(it, new_data[0], 0);
      ed_accessor_1.set_data(it, new_data[1], 0);
    }
  }
  for (size_t i = 0; i < src_lids.size(); ++i) {
    auto oe_edges = oe_view.get_edges(src_lids[i]);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      auto str_data = ed_accessor_0.get_data(it);
      auto int_data = ed_accessor_1.get_data(it);
      CHECK_EQ(int_data.as_int32(), 1);
      CHECK_EQ(str_data.as_string_view(), new_data[0].as_string_view());
    }
  }
}

TEST_F(EdgeTableTest, TestAddPropertiesTransitionFromEmptyToBundledUnbundled) {
  this->InitIndexers(4, 4);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_empty_);
  this->OpenEdgeTableInMemory(4, 4);
  this->ExpectBundledStats(0);

  std::vector<std::pair<int64_t, int64_t>> endpoints = {{0, 1}, {1, 2}, {2, 3}};
  std::vector<int64_t> src_list, dst_list;
  for (const auto& [src_oid, dst_oid] : endpoints) {
    src_list.emplace_back(src_oid);
    dst_list.emplace_back(dst_oid);
  }
  auto src_arrs = convert_to_arrow_arrays(src_list, src_list.size());
  auto dst_arrs = convert_to_arrow_arrays(dst_list, dst_list.size());
  auto batches =
      convert_to_record_batches({"src", "dst"}, {src_arrs, dst_arrs});
  this->BatchInsert(std::move(batches));
  this->ExpectBundledStats(endpoints.size());

  schema_.AddEdgeProperties("person", "comment", "create0", {"weight"},
                            {neug::DataTypeId::kInt32},
                            {neug::Property::from_int32(7)});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_empty_));
  this->edge_table->AddProperties({"weight"}, {neug::DataTypeId::kInt32},
                                  {neug::Property::from_int32(7)});
  this->ExpectBundledStats(endpoints.size());

  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), endpoints.size());
  std::vector<int> weights;
  this->OutputOutgoingEdgeData<int>(weights, neug::MAX_TIMESTAMP, 0);
  ASSERT_EQ(weights.size(), endpoints.size());
  for (auto weight : weights) {
    EXPECT_EQ(weight, 7);
  }

  schema_.AddEdgeProperties("person", "comment", "create0", {"tag"},
                            {neug::DataTypeId::kVarchar},
                            {neug::Property::from_string_view("new-tag")});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_empty_));
  this->edge_table->AddProperties(
      {"tag"}, {neug::DataTypeId::kVarchar},
      {neug::Property::from_string_view("new-tag")});
  this->ExpectUnbundledStats(endpoints.size(), 4096);

  std::vector<int> weights_after;
  std::vector<std::string_view> tags;
  this->OutputOutgoingEdgeData<int>(weights_after, neug::MAX_TIMESTAMP, 0);
  this->OutputOutgoingEdgeData<std::string_view>(tags, neug::MAX_TIMESTAMP, 1);
  ASSERT_EQ(weights_after.size(), endpoints.size());
  ASSERT_EQ(tags.size(), endpoints.size());
  for (size_t i = 0; i < endpoints.size(); ++i) {
    EXPECT_EQ(weights_after[i], 7);
    EXPECT_EQ(tags[i], "new-tag");
  }
}

TEST_F(EdgeTableTest, TestAddStringPropertyTransitionFromEmptyToUnbundled) {
  this->InitIndexers(4, 4);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_empty_);
  this->OpenEdgeTableInMemory(4, 4);
  this->ExpectBundledStats(0);

  std::vector<int64_t> src_list = {0, 1, 2};
  std::vector<int64_t> dst_list = {1, 2, 3};
  auto src_arrs = convert_to_arrow_arrays(src_list, src_list.size());
  auto dst_arrs = convert_to_arrow_arrays(dst_list, dst_list.size());
  auto batches =
      convert_to_record_batches({"src", "dst"}, {src_arrs, dst_arrs});
  this->BatchInsert(std::move(batches));
  this->ExpectBundledStats(src_list.size());

  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_empty_));
  schema_.get_edge_schema(src_label_, dst_label_, edge_label_empty_)
      ->add_properties({"tag"}, {neug::DataTypeId::kVarchar},
                       {neug::Property::from_string_view("seed")});
  this->edge_table->AddProperties({"tag"}, {neug::DataTypeId::kVarchar},
                                  {neug::Property::from_string_view("seed")});
  schema_.get_edge_schema(src_label_, dst_label_, edge_label_empty_)
      ->add_properties({"desc"}, {neug::DataTypeId::kVarchar},
                       {neug::Property::from_string_view("unknown")});
  this->edge_table->AddProperties(
      {"desc"}, {neug::DataTypeId::kVarchar},
      {neug::Property::from_string_view("unknown")});
  this->ExpectUnbundledStats(src_list.size(), 4096);

  std::vector<std::string_view> tags, descs;
  this->OutputOutgoingEdgeData<std::string_view>(tags, neug::MAX_TIMESTAMP, 0);
  this->OutputIncomingEdgeData<std::string_view>(descs, neug::MAX_TIMESTAMP, 1);
  ASSERT_EQ(tags.size(), src_list.size());
  for (auto tag : tags) {
    EXPECT_EQ(tag, "seed");
  }
  ASSERT_EQ(descs.size(), dst_list.size());
  for (auto desc : descs) {
    EXPECT_EQ(desc, "unknown");
  }
}

TEST_F(EdgeTableTest,
       TestDeletePropertiesTransitionFromUnbundledToBundledEmpty) {
  this->InitIndexers(4, 4);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTableInMemory(4, 4);
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size(), 100);
  this->ExpectUnbundledStats(0, 4096);

  std::vector<std::tuple<int64_t, int64_t, std::string, int>> input = {
      {0, 1, "a", 11}, {1, 2, "b", 22}, {2, 3, "c", 33}};
  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);
  for (const auto& [src_oid, dst_oid, data0, data1] : input) {
    this->edge_table->AddEdge(
        this->GetSrcLid(neug::Property::from_int64(src_oid)),
        this->GetDstLid(neug::Property::from_int64(dst_oid)),
        {neug::Property::from_string_view(data0),
         neug::Property::from_int32(data1)},
        0, allocator, false);
  }
  this->ExpectUnbundledStats(input.size(), 4096);

  this->edge_table->DeleteProperties({"data1"});
  schema_.DeleteEdgeProperties("person", "comment", "create3", {"data1"});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_str_int_));
  this->ExpectUnbundledStats(input.size(), 4096);

  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  std::vector<std::string_view> remaining_prop;
  this->OutputOutgoingEdgeData<std::string_view>(remaining_prop,
                                                 neug::MAX_TIMESTAMP, 0);
  ASSERT_EQ(srcs.size(), input.size());
  ASSERT_EQ(remaining_prop.size(), input.size());
  std::vector<std::tuple<int64_t, int64_t, std::string>> output;
  for (size_t i = 0; i < srcs.size(); ++i) {
    output.emplace_back(srcs[i], dsts[i], std::string(remaining_prop[i]));
  }
  std::sort(output.begin(), output.end());
  std::vector<std::tuple<int64_t, int64_t, std::string>> expected;
  for (const auto& [src_oid, dst_oid, data0, data1] : input) {
    expected.emplace_back(src_oid, dst_oid, data0);
  }
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(output, expected);

  this->edge_table->DeleteProperties({"data0"});
  schema_.DeleteEdgeProperties("person", "comment", "create3", {"data0"});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_str_int_));
  this->ExpectBundledStats(input.size());

  srcs.clear();
  dsts.clear();
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), input.size());
  ASSERT_EQ(dsts.size(), input.size());

  this->edge_table->AddEdge(this->GetSrcLid(neug::Property::from_int64(3)),
                            this->GetDstLid(neug::Property::from_int64(0)), {},
                            0, allocator, false);
  this->ExpectBundledStats(input.size() + 1);
  srcs.clear();
  dsts.clear();
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), input.size() + 1);
  ASSERT_EQ(dsts.size(), input.size() + 1);
}

TEST_F(EdgeTableTest, TestDeletePropertiesTransitionFromUnbundledToBundled) {
  this->InitIndexers(4, 4);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTableInMemory(4, 4);
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size(), 100);
  this->ExpectUnbundledStats(0, 4096);

  std::vector<std::tuple<int64_t, int64_t, std::string, int>> input = {
      {0, 1, "a", 11}, {1, 2, "b", 22}, {2, 3, "c", 33}};
  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);
  for (const auto& [src_oid, dst_oid, data0, data1] : input) {
    this->edge_table->AddEdge(
        this->GetSrcLid(neug::Property::from_int64(src_oid)),
        this->GetDstLid(neug::Property::from_int64(dst_oid)),
        {neug::Property::from_string_view(data0),
         neug::Property::from_int32(data1)},
        0, allocator, false);
  }
  this->ExpectUnbundledStats(input.size(), 4096);

  this->edge_table->DeleteProperties({"data0"});
  schema_.DeleteEdgeProperties("person", "comment", "create3", {"data0"});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_str_int_));

  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  std::vector<int> remaining_prop;
  this->OutputOutgoingEdgeData<int>(remaining_prop, neug::MAX_TIMESTAMP, 0);
  ASSERT_EQ(srcs.size(), input.size());
  ASSERT_EQ(remaining_prop.size(), input.size());
  std::vector<std::tuple<int64_t, int64_t, int>> output;
  for (size_t i = 0; i < srcs.size(); ++i) {
    output.emplace_back(srcs[i], dsts[i], remaining_prop[i]);
  }
  std::sort(output.begin(), output.end());
  std::vector<std::tuple<int64_t, int64_t, int>> expected;
  for (const auto& [src_oid, dst_oid, data0, data1] : input) {
    expected.emplace_back(src_oid, dst_oid, data1);
  }
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(output, expected);

  srcs.clear();
  dsts.clear();
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  ASSERT_EQ(srcs.size(), input.size());
  ASSERT_EQ(dsts.size(), input.size());

  this->ExpectBundledStats(input.size());
  this->edge_table->AddEdge(this->GetSrcLid(neug::Property::from_int64(3)),
                            this->GetDstLid(neug::Property::from_int64(0)),
                            {Property::from_int32(44)}, 0, allocator, false);
  this->ExpectBundledStats(input.size() + 1);
}

TEST_F(EdgeTableTest, TestAddAndDeletePropertiesStayUnbundled) {
  this->InitIndexers(4, 4);
  this->ConstructEdgeTable(src_label_, dst_label_, edge_label_str_int_);
  this->OpenEdgeTableInMemory(4, 4);
  this->edge_table->EnsureCapacity(this->src_indexer.size(),
                                   this->dst_indexer.size(), 100);
  this->ExpectUnbundledStats(0, 4096);

  std::vector<std::tuple<int64_t, int64_t, std::string, int>> input = {
      {0, 1, "a", 11}, {1, 2, "b", 22}, {2, 3, "c", 33}};
  neug::Allocator allocator(neug::MemoryLevel::kInMemory, allocator_dir_);
  for (const auto& [src_oid, dst_oid, data0, data1] : input) {
    this->edge_table->AddEdge(
        this->GetSrcLid(neug::Property::from_int64(src_oid)),
        this->GetDstLid(neug::Property::from_int64(dst_oid)),
        {neug::Property::from_string_view(data0),
         neug::Property::from_int32(data1)},
        0, allocator, false);
  }
  this->ExpectUnbundledStats(input.size(), 4096);

  schema_.AddEdgeProperties("person", "comment", "create3", {"score"},
                            {neug::DataTypeId::kInt32},
                            {neug::Property::from_int32(99)});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_str_int_));
  this->edge_table->AddProperties({"score"}, {neug::DataTypeId::kInt32},
                                  {neug::Property::from_int32(99)});
  this->ExpectUnbundledStats(input.size(), 4096);

  std::vector<int> score;
  this->OutputOutgoingEdgeData<int>(score, neug::MAX_TIMESTAMP, 2);
  ASSERT_EQ(score.size(), input.size());
  for (auto value : score) {
    EXPECT_EQ(value, 99);
  }

  this->edge_table->DeleteProperties({"score"});
  schema_.DeleteEdgeProperties("person", "comment", "create3", {"score"});
  this->edge_table->SetEdgeSchema(
      schema_.get_edge_schema(src_label_, dst_label_, edge_label_str_int_));
  this->ExpectUnbundledStats(input.size(), 4096);

  std::vector<std::string_view> data0_after;
  std::vector<int> data1_after;
  this->OutputOutgoingEdgeData<std::string_view>(data0_after,
                                                 neug::MAX_TIMESTAMP, 0);
  this->OutputOutgoingEdgeData<int>(data1_after, neug::MAX_TIMESTAMP, 1);
  ASSERT_EQ(data0_after.size(), input.size());
  ASSERT_EQ(data1_after.size(), input.size());
  std::vector<std::tuple<int64_t, int64_t, std::string, int>> output;
  std::vector<int64_t> srcs, dsts;
  this->OutputOutgoingEndpoints(srcs, dsts, neug::MAX_TIMESTAMP);
  for (size_t i = 0; i < srcs.size(); ++i) {
    output.emplace_back(srcs[i], dsts[i], std::string(data0_after[i]),
                        data1_after[i]);
  }
  std::sort(output.begin(), output.end());
  std::sort(input.begin(), input.end());
  EXPECT_EQ(output, input);
}

template <typename EDATA_T, typename ARROW_COL_T>
struct TypePair {
  using EdType = EDATA_T;
  using ArrowType = ARROW_COL_T;
};
using Datatypes = ::testing::Types<
    TypePair<int32_t, typename TypeConverter<int32_t>::ArrowArrayType>,
    TypePair<uint32_t, typename TypeConverter<uint32_t>::ArrowArrayType>,
    TypePair<int64_t, typename TypeConverter<int64_t>::ArrowArrayType>,
    TypePair<uint64_t, typename TypeConverter<uint64_t>::ArrowArrayType>,
    TypePair<float, typename TypeConverter<float>::ArrowArrayType>,
    TypePair<double, typename TypeConverter<double>::ArrowArrayType>,
    TypePair<neug::Date, typename TypeConverter<neug::Date>::ArrowArrayType>,
    TypePair<neug::DateTime,
             typename TypeConverter<neug::DateTime>::ArrowArrayType>,
    TypePair<neug::Interval,
             typename TypeConverter<neug::Interval>::ArrowArrayType>>;

template <typename T>
class EdgeTableToolsTest : public ::testing::Test {};
TYPED_TEST_SUITE(EdgeTableToolsTest, Datatypes);

TYPED_TEST(EdgeTableToolsTest, TestBatchAddEdges) {
  using EdType = typename TypeParam::EdType;
  const char* var = std::getenv("TEST_PATH");
  std::string test_path = var ? var : "/workspaces/neug/tests";
  std::string resource_path = test_path + "/storage/resources";
  std::shared_ptr<EdgeSchema> edge_schema = std::make_shared<EdgeSchema>();
  edge_schema->ie_mutable = true;
  edge_schema->oe_mutable = true;
  edge_schema->ie_strategy = EdgeStrategy::kMultiple;
  edge_schema->oe_strategy = EdgeStrategy::kMultiple;
  std::vector<std::string> property_name = {"test_property"};

  std::string file_path;
  std::vector<DataType> column_types = {DataTypeId::kUInt32,
                                        DataTypeId::kUInt32};
  std::unordered_map<std::string, std::string> csv_options;
  csv_options.insert({"HEADER", "FALSE"});
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  if constexpr (std::is_same_v<EdType, int32_t>) {
    file_path = resource_path + "/edges_i32.csv";
    std::vector<DataType> property_type = {DataTypeId::kInt32};
    column_types.emplace_back(DataTypeId::kInt32);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, int64_t>) {
    file_path = resource_path + "/edges_i64.csv";
    std::vector<DataType> property_type = {DataTypeId::kInt64};
    column_types.emplace_back(DataTypeId::kInt64);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, uint32_t>) {
    file_path = resource_path + "/edges_u32.csv";
    std::vector<DataType> property_type = {DataTypeId::kUInt32};
    column_types.emplace_back(DataTypeId::kUInt32);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, uint64_t>) {
    file_path = resource_path + "/edges_u64.csv";
    std::vector<DataType> property_type = {DataTypeId::kUInt64};
    column_types.emplace_back(DataTypeId::kUInt64);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, float>) {
    file_path = resource_path + "/edges_float.csv";
    std::vector<DataType> property_type = {DataTypeId::kFloat};
    column_types.emplace_back(DataTypeId::kFloat);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, double>) {
    file_path = resource_path + "/edges_double.csv";
    std::vector<DataType> property_type = {DataTypeId::kDouble};
    column_types.emplace_back(DataTypeId::kDouble);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, Date>) {
    file_path = resource_path + "/edges_date.csv";
    std::vector<DataType> property_type = {DataTypeId::kDate};
    column_types.emplace_back(DataTypeId::kDate);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, DateTime>) {
    file_path = resource_path + "/edges_datetime.csv";
    std::vector<DataType> property_type = {DataTypeId::kTimestampMs};
    column_types.emplace_back(DataTypeId::kTimestampMs);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else if constexpr (std::is_same_v<EdType, Interval>) {
    file_path = resource_path + "/edges_interval.csv";
    std::vector<DataType> property_type = {DataTypeId::kInterval};
    column_types.emplace_back(DataTypeId::kInterval);
    edge_schema->add_properties(property_name, property_type);
    suppliers = execution::ops::create_csv_record_suppliers(
        file_path, column_types, csv_options);
  } else {
    FAIL();
  }
  EXPECT_EQ(suppliers.size(), 1);

  LFIndexer<vid_t> indexer;
  indexer.init(DataTypeId::kUInt32);
  indexer.open_in_memory("/tmp");
  indexer.reserve(10);
  for (uint32_t i = 0; i < 10; i++) {
    Property oid;
    oid.set_uint32(i);
    indexer.insert(oid, false);
  }

  EdgeTable e_table = EdgeTable(edge_schema);
  e_table.Open("/tmp/", MemoryLevel::kInMemory);
  e_table.BatchAddEdges(indexer, indexer, suppliers[0]);
  EXPECT_EQ(e_table.EdgeNum(), 10);
  EXPECT_EQ(e_table.PropTableSize(), 0);
  EXPECT_EQ(e_table.Capacity(), neug::CsrBase::INFINITE_CAPACITY);

  std::vector<std::string> new_property_name = {"new_property"};
  std::vector<DataType> new_property_type = {DataTypeId::kInt32};
  edge_schema->add_properties(new_property_name, new_property_type);
  e_table.AddProperties(new_property_name, new_property_type);
  EXPECT_EQ(e_table.PropertyNum(), 2);
  EXPECT_EQ(e_table.PropTableSize(), 10);
}

TYPED_TEST(EdgeTableToolsTest, TestAddProperties) {
  using EdType = typename TypeParam::EdType;
  const char* var = std::getenv("TEST_PATH");
  std::string test_path = var ? var : "/workspaces/neug/tests";
  std::string resource_path = test_path + "/storage/resources";
  std::shared_ptr<EdgeSchema> edge_schema = std::make_shared<EdgeSchema>();
  edge_schema->ie_mutable = true;
  edge_schema->oe_mutable = true;
  edge_schema->ie_strategy = EdgeStrategy::kMultiple;
  edge_schema->oe_strategy = EdgeStrategy::kMultiple;

  std::string file_path = resource_path + "/edges_empty.csv";
  std::vector<DataType> column_types = {DataTypeId::kUInt32,
                                        DataTypeId::kUInt32};
  std::unordered_map<std::string, std::string> csv_options;
  csv_options.insert({"HEADER", "FALSE"});
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  suppliers = execution::ops::create_csv_record_suppliers(
      file_path, column_types, csv_options);
  EXPECT_EQ(suppliers.size(), 1);

  LFIndexer<vid_t> indexer;
  indexer.init(DataTypeId::kUInt32);
  indexer.open_in_memory("/tmp");
  indexer.reserve(10);
  for (uint32_t i = 0; i < 10; i++) {
    Property oid;
    oid.set_uint32(i);
    indexer.insert(oid, false);
  }

  std::vector<std::string> new_property_name = {"new_property"};
  std::vector<DataType> new_property_type;
  EdgeTable e_table = EdgeTable(edge_schema);
  e_table.Open("/tmp/", MemoryLevel::kInMemory);
  e_table.BatchAddEdges(indexer, indexer, suppliers[0]);
  EXPECT_EQ(e_table.EdgeNum(), 10);
  EXPECT_EQ(e_table.PropTableSize(), 0);
  EXPECT_EQ(e_table.Capacity(), neug::CsrBase::INFINITE_CAPACITY);
  if constexpr (std::is_same_v<EdType, int32_t>) {
    new_property_type = {DataTypeId::kInt32};
  } else if constexpr (std::is_same_v<EdType, int64_t>) {
    new_property_type = {DataTypeId::kInt64};
  } else if constexpr (std::is_same_v<EdType, uint32_t>) {
    new_property_type = {DataTypeId::kUInt32};
  } else if constexpr (std::is_same_v<EdType, uint64_t>) {
    new_property_type = {DataTypeId::kUInt64};
  } else if constexpr (std::is_same_v<EdType, float>) {
    new_property_type = {DataTypeId::kFloat};
  } else if constexpr (std::is_same_v<EdType, double>) {
    new_property_type = {DataTypeId::kDouble};
  } else if constexpr (std::is_same_v<EdType, Date>) {
    new_property_type = {DataTypeId::kDate};
  } else if constexpr (std::is_same_v<EdType, DateTime>) {
    new_property_type = {DataTypeId::kTimestampMs};
  } else if constexpr (std::is_same_v<EdType, Interval>) {
    new_property_type = {DataTypeId::kInterval};
  } else {
    FAIL();
  }

  edge_schema->add_properties(new_property_name, new_property_type);
  e_table.AddProperties(new_property_name, new_property_type);
  EXPECT_EQ(e_table.PropTableSize(), 0);
  EXPECT_EQ(e_table.Capacity(), neug::CsrBase::INFINITE_CAPACITY);
}

}  // namespace test
}  // namespace neug
