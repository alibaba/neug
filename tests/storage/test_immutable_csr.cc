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
#include <iostream>
#include <string>
#include "neug/storages/csr/generic_view_utils.h"
#include "neug/storages/csr/immutable_csr.h"
#include "unittest/utils.h"

namespace neug {
namespace test {
using Datatypes =
    ::testing::Types<neug::EmptyType, int32_t, uint32_t, int64_t, uint64_t,
                     double, float, Date, DateTime, Interval>;

template <typename EDATA_T>
class IMMutableCsrTest : public ::testing::Test {
 protected:
  void SetUp() override {
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

  void load_csr_data(ImmutableCsr<EDATA_T>& csr) {
    auto work_dir = this->WorkDirectory();
    auto snapshot_dir = this->SnapshotDirectory();
    csr.open("csr", snapshot_dir.string(), work_dir.string());

    auto edges = generate_random_edges<EDATA_T>(500, 1000, 10000, false);
    csr.resize(500);
    std::vector<neug::vid_t> src_list, dst_list;
    std::vector<EDATA_T> edata_list;
    for (size_t i = 0; i < edges.size(); ++i) {
      src_list.push_back(std::get<0>(edges[i]));
      dst_list.push_back(std::get<1>(edges[i]));
      edata_list.push_back(std::get<2>(edges[i]));
    }
    csr.batch_put_edges(src_list, dst_list, edata_list, 0);
  }

  void load_single_csr_data(SingleImmutableCsr<EDATA_T>& csr) {
    auto work_dir = this->WorkDirectory();
    auto snapshot_dir = this->SnapshotDirectory();
    csr.open("csr", snapshot_dir.string(), work_dir.string());

    auto edges = generate_random_edges<EDATA_T>(500, 1000, 10000, true);
    csr.resize(500);
    std::vector<neug::vid_t> src_list, dst_list;
    std::vector<EDATA_T> edata_list;
    for (size_t i = 0; i < edges.size(); ++i) {
      src_list.push_back(std::get<0>(edges[i]));
      dst_list.push_back(std::get<1>(edges[i]));
      edata_list.push_back(std::get<2>(edges[i]));
    }
    csr.batch_put_edges(src_list, dst_list, edata_list, 0);
  }

  bool check_edge_data_ordered(GenericView& generic_view) {
    for (vid_t v = 0; v < 500; v++) {
      if constexpr (std::is_same_v<EDATA_T, int32_t>) {
        NbrList nbr_list = generic_view.get_edges(0);
        int32_t cur_value =
            *static_cast<const int32_t*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          int32_t next_value = *static_cast<const int32_t*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, int64_t>) {
        NbrList nbr_list = generic_view.get_edges(0);
        int64_t cur_value =
            *static_cast<const int64_t*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          int64_t next_value = *static_cast<const int64_t*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, uint32_t>) {
        NbrList nbr_list = generic_view.get_edges(0);
        uint32_t cur_value =
            *static_cast<const uint32_t*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          uint32_t next_value =
              *static_cast<const uint32_t*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, uint64_t>) {
        NbrList nbr_list = generic_view.get_edges(0);
        uint64_t cur_value =
            *static_cast<const uint64_t*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          uint64_t next_value =
              *static_cast<const uint64_t*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, float>) {
        NbrList nbr_list = generic_view.get_edges(0);
        float cur_value =
            *static_cast<const float*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          float next_value = *static_cast<const float*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, double>) {
        NbrList nbr_list = generic_view.get_edges(0);
        double cur_value =
            *static_cast<const double*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          double next_value = *static_cast<const double*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, Date>) {
        NbrList nbr_list = generic_view.get_edges(0);
        Date cur_value =
            *static_cast<const Date*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          Date next_value = *static_cast<const Date*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, DateTime>) {
        NbrList nbr_list = generic_view.get_edges(0);
        DateTime cur_value =
            *static_cast<const DateTime*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          DateTime next_value =
              *static_cast<const DateTime*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, Interval>) {
        NbrList nbr_list = generic_view.get_edges(0);
        Interval cur_value =
            *static_cast<const Interval*>(nbr_list.begin().get_data_ptr());
        for (auto nbr = ++nbr_list.begin(); nbr != nbr_list.end(); ++nbr) {
          Interval next_value =
              *static_cast<const Interval*>(nbr.get_data_ptr());
          if (next_value < cur_value) {
            return false;
          } else {
            cur_value = next_value;
          }
        }
      } else if constexpr (std::is_same_v<EDATA_T, neug::EmptyType>) {
        continue;
      } else {
        return false;
      }
    }
    return true;
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

 private:
  std::filesystem::path temp_dir_;

  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }
};
TYPED_TEST_SUITE(IMMutableCsrTest, Datatypes);

TYPED_TEST(IMMutableCsrTest, TestCsrType) {
  ImmutableCsr<TypeParam> immutable_csr;
  EXPECT_EQ(immutable_csr.csr_type(), CsrType::kImmutable);
  SingleImmutableCsr<TypeParam> single_immutable_csr;
  EXPECT_EQ(single_immutable_csr.csr_type(), CsrType::kSingleImmutable);
}

TYPED_TEST(IMMutableCsrTest, TestUnsortedSince) {
  timestamp_t sort_ts = 10;
  ImmutableCsr<TypeParam> immutable_csr;
  immutable_csr.batch_sort_by_edge_data(10);
  EXPECT_EQ(immutable_csr.unsorted_since(), sort_ts);
  SingleImmutableCsr<TypeParam> single_immutable_csr;
  single_immutable_csr.batch_sort_by_edge_data(10);
  EXPECT_EQ(single_immutable_csr.unsorted_since(),
            std::numeric_limits<timestamp_t>::max());
}

TYPED_TEST(IMMutableCsrTest, TestBasicFunction) {
  ImmutableCsr<TypeParam> immutable_csr;
  this->load_csr_data(immutable_csr);
  EXPECT_EQ(immutable_csr.size(), 500);
  EXPECT_EQ(immutable_csr.edge_num(), 10000);
  immutable_csr.compact();
  immutable_csr.reset_timestamp();

  SingleImmutableCsr<TypeParam> single_immutable_csr;
  this->load_single_csr_data(single_immutable_csr);
  EXPECT_EQ(single_immutable_csr.size(), 500);
  EXPECT_EQ(single_immutable_csr.edge_num(), 500);
  single_immutable_csr.compact();
  single_immutable_csr.reset_timestamp();
}

TYPED_TEST(IMMutableCsrTest, TestDumpAndOpen) {
  ImmutableCsr<TypeParam> immutable_csr;
  this->load_csr_data(immutable_csr);
  immutable_csr.dump("dumped_csr_data", this->WorkDirectory());
  std::filesystem::create_directories(tmp_dir(this->WorkDirectory()));
  ImmutableCsr<TypeParam> fmap_immutable_csr, memory_immutable_csr,
      hugepage_immutable_csr;
  fmap_immutable_csr.open("dumped_csr_data", this->WorkDirectory(),
                          this->WorkDirectory());
  EXPECT_EQ(fmap_immutable_csr.edge_num(), 10000);
  memory_immutable_csr.open_in_memory(std::string(this->WorkDirectory()) +
                                      "/dumped_csr_data");

  EXPECT_EQ(memory_immutable_csr.edge_num(), 10000);
  hugepage_immutable_csr.open_with_hugepages(
      std::string(this->WorkDirectory()) + "/dumped_csr_data");
  EXPECT_EQ(hugepage_immutable_csr.edge_num(), 10000);

  SingleImmutableCsr<TypeParam> single_immutable_csr;
  this->load_single_csr_data(single_immutable_csr);
  single_immutable_csr.dump("dumped_csr_data", this->WorkDirectory());
  std::filesystem::create_directories(tmp_dir(this->WorkDirectory()));
  SingleImmutableCsr<TypeParam> fmap_single_immutable_csr,
      memory_single_immutable_csr, hugepage_single_immutable_csr;
  fmap_single_immutable_csr.open("dumped_csr_data", this->WorkDirectory(),
                                 this->WorkDirectory());
  EXPECT_EQ(fmap_single_immutable_csr.edge_num(), 500);
  memory_single_immutable_csr.open_in_memory(
      std::string(this->WorkDirectory()) + "/dumped_csr_data");
  EXPECT_EQ(memory_single_immutable_csr.edge_num(), 500);
  hugepage_single_immutable_csr.open_with_hugepages(
      std::string(this->WorkDirectory()) + "/dumped_csr_data");
  EXPECT_EQ(hugepage_single_immutable_csr.edge_num(), 500);
}

TYPED_TEST(IMMutableCsrTest, TestResize) {
  ImmutableCsr<TypeParam> immutable_csr;
  this->load_csr_data(immutable_csr);
  immutable_csr.resize(498);
  EXPECT_EQ(immutable_csr.size(), 498);
  immutable_csr.close();

  SingleImmutableCsr<TypeParam> single_immutable_csr;
  this->load_single_csr_data(single_immutable_csr);
  single_immutable_csr.resize(498);
  EXPECT_EQ(single_immutable_csr.size(), 498);
  single_immutable_csr.close();
}

TYPED_TEST(IMMutableCsrTest, TestSortByEdgeData) {
  timestamp_t sort_ts = 10;

  ImmutableCsr<TypeParam> immutable_csr;
  this->load_csr_data(immutable_csr);
  immutable_csr.batch_sort_by_edge_data(sort_ts);
  GenericView immutable_view = immutable_csr.get_generic_view(sort_ts);
  EXPECT_EQ(immutable_view.type(), CsrViewType::kMultipleImmutable);
  EXPECT_TRUE(this->check_edge_data_ordered(immutable_view));
  immutable_csr.close();

  SingleImmutableCsr<TypeParam> single_immutable_csr;
  this->load_single_csr_data(single_immutable_csr);
  single_immutable_csr.batch_sort_by_edge_data(sort_ts);
  GenericView single_immutable_view =
      single_immutable_csr.get_generic_view(sort_ts);
  EXPECT_EQ(single_immutable_view.type(), CsrViewType::kSingleImmutable);
  single_immutable_csr.close();
}

TYPED_TEST(IMMutableCsrTest, TestBatchDeleteVertices) {
  {
    ImmutableCsr<TypeParam> immutable_csr;
    this->load_csr_data(immutable_csr);
    size_t count = 0;
    std::set<vid_t> delete_src_vertices, delete_dst_vertices;
    auto view = immutable_csr.get_generic_view(1);
    for (vid_t i = 0; i < 100; i++) {
      auto edges = view.get_edges(i);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        count++;
      }
      delete_src_vertices.insert(i);
    }
    immutable_csr.batch_delete_vertices(delete_src_vertices,
                                        delete_dst_vertices);
    EXPECT_EQ(immutable_csr.edge_num(), 10000 - count);
  }

  {
    SingleImmutableCsr<TypeParam> single_immutable_csr;
    this->load_single_csr_data(single_immutable_csr);
    std::set<vid_t> delete_src_vertices, delete_dst_vertices;
    for (vid_t i = 0; i < 100; i++) {
      delete_src_vertices.insert(i);
    }
    single_immutable_csr.batch_delete_vertices(delete_src_vertices,
                                               delete_dst_vertices);
    EXPECT_EQ(single_immutable_csr.edge_num(), 400);
  }
}

TYPED_TEST(IMMutableCsrTest, TestBatchDeleteEdges) {
  ImmutableCsr<TypeParam> immutable_csr;
  this->load_csr_data(immutable_csr);
  std::vector<std::pair<vid_t, int32_t>> edges_to_delete;
  auto view = immutable_csr.get_generic_view(1);
  for (vid_t i = 0; i < immutable_csr.size(); i++) {
    auto edges = view.get_edges(i);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                     reinterpret_cast<const char*>(edges.start_ptr)) /
                    it.cfg.stride;
      edges_to_delete.emplace_back(i, static_cast<int32_t>(offset));
    }
    if (edges_to_delete.size() > 100) {
      break;
    }
  }
  immutable_csr.batch_delete_edges(edges_to_delete);
  EXPECT_EQ(immutable_csr.edge_num(), 10000 - edges_to_delete.size());

  SingleImmutableCsr<TypeParam> single_immutable_csr;
  this->load_single_csr_data(single_immutable_csr);
  edges_to_delete.clear();
  view = single_immutable_csr.get_generic_view(1);
  for (size_t i = 0; i < 100; ++i) {
    auto edges = view.get_edges(i);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                     reinterpret_cast<const char*>(edges.start_ptr)) /
                    it.cfg.stride;
      edges_to_delete.emplace_back(i, offset);
      assert(offset == 0);
      break;
    }
  }

  single_immutable_csr.batch_delete_edges(edges_to_delete);
  EXPECT_EQ(single_immutable_csr.edge_num(), 400);
}

TYPED_TEST(IMMutableCsrTest, TestBatchDeleteEdgesById) {
  ImmutableCsr<TypeParam> immutable_csr;
  this->load_csr_data(immutable_csr);
  std::vector<vid_t> src_ids, dst_ids;
  auto view = immutable_csr.get_generic_view(1);
  for (vid_t i = 0; i < immutable_csr.size(); i++) {
    auto edges = view.get_edges(i);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      src_ids.emplace_back(i);
      dst_ids.emplace_back(it.get_vertex());
    }
    if (src_ids.size() > 100) {
      break;
    }
  }
  immutable_csr.batch_delete_edges(src_ids, dst_ids);
  EXPECT_EQ(immutable_csr.edge_num(), 10000 - src_ids.size());

  SingleImmutableCsr<TypeParam> single_immutable_csr;
  this->load_single_csr_data(single_immutable_csr);
  src_ids.clear();
  dst_ids.clear();
  view = single_immutable_csr.get_generic_view(1);
  for (size_t i = 0; i < 100; ++i) {
    auto edges = view.get_edges(i);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      src_ids.emplace_back(i);
      dst_ids.emplace_back(it.get_vertex());
      break;
    }
  }

  single_immutable_csr.batch_delete_edges(src_ids, dst_ids);
  EXPECT_EQ(single_immutable_csr.edge_num(), 400);
}

TYPED_TEST(IMMutableCsrTest, TestDeleteRevertEdge) {
  {
    ImmutableCsr<TypeParam> immutable_csr;
    this->load_csr_data(immutable_csr);

    size_t edge_num = immutable_csr.edge_num();
    std::vector<std::tuple<vid_t, vid_t, int32_t>> edges_to_delete;
    auto oe_view = immutable_csr.get_generic_view(0);
    std::set<size_t> deleted_indices;
    for (vid_t i = 0; i < 5; i++) {
      auto edges = oe_view.get_edges(i);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                       reinterpret_cast<const char*>(edges.start_ptr)) /
                      it.cfg.stride;
        edges_to_delete.emplace_back(i, it.get_vertex(),
                                     static_cast<int32_t>(offset));
      }
    }
    for (const auto& edge : edges_to_delete) {
      immutable_csr.delete_edge(std::get<0>(edge), std::get<2>(edge), 0);
    }
    EXPECT_EQ(immutable_csr.edge_num(), edge_num - edges_to_delete.size());
    for (const auto& edge : edges_to_delete) {
      immutable_csr.revert_delete_edge(std::get<0>(edge), std::get<1>(edge),
                                       std::get<2>(edge), 0);
    }
  }
  {
    SingleImmutableCsr<TypeParam> single_immutable_csr;
    this->load_single_csr_data(single_immutable_csr);

    size_t edge_num = single_immutable_csr.edge_num();
    std::vector<std::tuple<vid_t, vid_t, int32_t>> edges_to_delete;
    auto oe_view = single_immutable_csr.get_generic_view(0);
    for (vid_t i = 0; i < 5; i++) {
      auto edges = oe_view.get_edges(i);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                       reinterpret_cast<const char*>(edges.start_ptr)) /
                      it.cfg.stride;
        edges_to_delete.emplace_back(i, it.get_vertex(),
                                     static_cast<int32_t>(offset));
      }
    }
    for (const auto& edge : edges_to_delete) {
      single_immutable_csr.delete_edge(std::get<0>(edge), std::get<2>(edge), 0);
    }
    EXPECT_EQ(single_immutable_csr.edge_num(),
              edge_num - edges_to_delete.size());
    for (const auto& edge : edges_to_delete) {
      single_immutable_csr.revert_delete_edge(
          std::get<0>(edge), std::get<1>(edge), std::get<2>(edge), 0);
    }
  }
}

}  // namespace test
}  // namespace neug