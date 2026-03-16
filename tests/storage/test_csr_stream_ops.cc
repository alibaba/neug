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
#include <thread>
#include <vector>

#include "neug/storages/csr/mutable_csr.h"
#include "neug/utils/allocators.h"
#include "unittest/utils.h"

using StreamCsrTypes = ::testing::Types<
    neug::MutableCsr<int32_t>, neug::MutableCsr<int64_t>,
    neug::MutableCsr<float>, neug::MutableCsr<neug::EmptyType>,
    neug::MutableCsr<neug::DateTime>, neug::SingleMutableCsr<uint32_t>,
    neug::SingleMutableCsr<uint64_t>, neug::SingleMutableCsr<double>,
    neug::SingleMutableCsr<neug::Date>>;

template <typename T>
class CsrStreamTest : public ::testing::Test {
 protected:
  using CsrType = T;

  void SetUp() override {
    csr = std::make_unique<CsrType>();
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("csr_batch_ops_" + std::to_string(::getpid()) + "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    std::filesystem::create_directories(temp_dir_ / "snapshot");
    std::filesystem::create_directories(temp_dir_ / "work");
    std::filesystem::create_directories(temp_dir_ / "work" / "runtime" / "tmp");
  }

  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  std::filesystem::path WorkDirectory() const { return temp_dir_ / "work"; }
  std::filesystem::path SnapshotDirectory() const {
    return temp_dir_ / "snapshot";
  }

  void CreateDirectory(const std::string& name) {
    std::filesystem::create_directories(temp_dir_ / name);
  }

  std::filesystem::path GetDirectory(const std::string& name) const {
    return temp_dir_ / name;
  }

  void ClearWorkDirectory() {
    auto work_dir = WorkDirectory();
    if (std::filesystem::exists(work_dir)) {
      std::filesystem::remove_all(work_dir);
    }
    std::filesystem::create_directories(work_dir);
  }

  void CheckEqual(const std::vector<std::tuple<neug::vid_t, neug::vid_t,
                                               typename T::data_t>>& expected,
                  neug::timestamp_t ts) {
    std::vector<std::tuple<neug::vid_t, neug::vid_t, typename T::data_t>>
        actual;
    auto view = this->csr->get_generic_view(ts);
    auto ed_accessor = neug::EdgeDataAccessor(
        neug::PropUtils<typename T::data_t>::prop_type(), nullptr);
    for (neug::vid_t src = 0; src < this->csr->size(); ++src) {
      auto es = view.get_edges(src);
      for (auto it = es.begin(); it != es.end(); ++it) {
        auto edata = ed_accessor.get_typed_data<typename T::data_t>(it);
        actual.emplace_back(src, it.get_vertex(), edata);
      }
    }
    EXPECT_EQ(expected.size(), actual.size());
    std::sort(actual.begin(), actual.end());
    using V = typename T::data_t;
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(std::get<0>(expected[i]), std::get<0>(actual[i]));
      EXPECT_EQ(std::get<1>(expected[i]), std::get<1>(actual[i]));
      if constexpr (std::is_floating_point<V>::value) {
        EXPECT_FLOAT_EQ(std::get<2>(expected[i]), std::get<2>(actual[i]));
      } else {
        EXPECT_EQ(std::get<2>(expected[i]), std::get<2>(actual[i]));
      }
    }
  }

  neug::timestamp_t ParallelInsert(
      const std::vector<
          std::tuple<neug::vid_t, neug::vid_t, typename T::data_t>>& edges,
      int thread_num, neug::timestamp_t start_ts) {
    std::vector<std::thread> threads;
    if (this->allocators.size() < static_cast<size_t>(thread_num)) {
      this->allocators.resize(
          thread_num, neug::Allocator(neug::MemoryStrategy::kMemoryOnly, ""));
    }
    std::atomic<size_t> counter(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back(
          [this, &counter, &edges, start_ts](int thread_idx) {
            auto& alloc = this->allocators[thread_idx];
            while (true) {
              size_t cur = counter.fetch_add(1);
              if (cur >= edges.size()) {
                break;
              }
              auto& e = edges[cur];
              this->csr->put_edge(std::get<0>(e), std::get<1>(e),
                                  std::get<2>(e), start_ts + cur, alloc);
            }
          },
          i);
    }
    for (auto& th : threads) {
      th.join();
    }
    return start_ts + counter.load();
  }

  std::unique_ptr<CsrType> csr = nullptr;
  std::vector<neug::Allocator> allocators;

 private:
  std::filesystem::path temp_dir_;

  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }
};

TYPED_TEST_SUITE(CsrStreamTest, StreamCsrTypes);

template <typename EDATA_T>
static std::pair<std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>>,
                 std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>>>
generate_two_part_edges(neug::vid_t src_num, neug::vid_t dst_num,
                        size_t edge_num, double init_percent, bool is_single) {
  neug::vid_t init_src_num = src_num * init_percent;
  auto edges =
      generate_random_edges<EDATA_T>(src_num, dst_num, edge_num, is_single);
  std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>> part0, part1;
  for (auto& e : edges) {
    if (std::get<0>(e) < init_src_num) {
      part0.push_back(e);
    } else {
      part1.push_back(e);
    }
  }
  size_t part0_init_size = part0.size() * init_percent;
  for (size_t i = part0_init_size; i < part0.size(); ++i) {
    part1.push_back(part0[i]);
  }
  part0.resize(part0_init_size);
  return {part0, part1};
}

TYPED_TEST(CsrStreamTest, ParallelInsertMemoryEmpty) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  auto edges = generate_random_edges<typename TypeParam::data_t>(
      1000, 1000, 20000,
      this->csr->csr_type() == neug::CsrType::kSingleMutable);

  this->csr->open_in_memory(snapshot_dir.string() + "/csr");
  this->csr->resize(1000);

  auto ts = this->ParallelInsert(edges, 16, 1);

  std::sort(edges.begin(), edges.end());
  this->CheckEqual(edges, ts);
}

TYPED_TEST(CsrStreamTest, ParallelInsertFileEmpty) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  auto edges = generate_random_edges<typename TypeParam::data_t>(
      1000, 1000, 20000,
      this->csr->csr_type() == neug::CsrType::kSingleMutable);

  this->csr->open("csr", snapshot_dir.string(), work_dir.string());
  this->csr->resize(1000);

  auto ts = this->ParallelInsert(edges, 16, 1);

  std::sort(edges.begin(), edges.end());
  this->CheckEqual(edges, ts);
}

TYPED_TEST(CsrStreamTest, ParallelInsertMemory) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  auto pair = generate_two_part_edges<typename TypeParam::data_t>(
      1000, 1000, 20000, 0.6,
      this->csr->csr_type() == neug::CsrType::kSingleMutable);
  auto& init_edges = pair.first;
  auto& insert_edges = pair.second;

  std::vector<neug::vid_t> src_list, dst_list;
  std::vector<typename TypeParam::data_t> data_list;
  for (auto& e : init_edges) {
    src_list.push_back(std::get<0>(e));
    dst_list.push_back(std::get<1>(e));
    data_list.push_back(std::get<2>(e));
  }

  this->csr->open_in_memory(snapshot_dir.string() + "/csr");
  this->csr->resize(1000);
  this->csr->batch_put_edges(src_list, dst_list, data_list, 0);
  std::sort(init_edges.begin(), init_edges.end());
  this->CheckEqual(init_edges, 0);

  auto ts = this->ParallelInsert(insert_edges, 16, 1);

  for (auto& e : insert_edges) {
    init_edges.push_back(e);
  }
  std::sort(init_edges.begin(), init_edges.end());
  this->CheckEqual(init_edges, ts);
}

TYPED_TEST(CsrStreamTest, ParallelInsertFile) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();

  auto pair = generate_two_part_edges<typename TypeParam::data_t>(
      1000, 1000, 20000, 0.6,
      this->csr->csr_type() == neug::CsrType::kSingleMutable);
  auto& init_edges = pair.first;
  auto& insert_edges = pair.second;

  std::vector<neug::vid_t> src_list, dst_list;
  std::vector<typename TypeParam::data_t> data_list;
  for (auto& e : init_edges) {
    src_list.push_back(std::get<0>(e));
    dst_list.push_back(std::get<1>(e));
    data_list.push_back(std::get<2>(e));
  }

  this->csr->open("csr", snapshot_dir.string(), work_dir.string());
  this->csr->resize(1000);
  this->csr->batch_put_edges(src_list, dst_list, data_list, 0);
  std::sort(init_edges.begin(), init_edges.end());
  this->CheckEqual(init_edges, 0);

  auto ts = this->ParallelInsert(insert_edges, 16, 1);

  for (auto& e : insert_edges) {
    init_edges.push_back(e);
  }
  std::sort(init_edges.begin(), init_edges.end());
  this->CheckEqual(init_edges, ts);
}