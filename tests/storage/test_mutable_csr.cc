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
#include "neug/storages/csr/mutable_csr.h"

static const size_t src_v_num = 5;
static const size_t single_src_v_num = 10;
static const size_t edge_num = 10;
static const std::vector<neug::vid_t> src_vid = {0, 0, 0, 1, 2, 2, 4, 4, 4, 4};
static const std::vector<neug::vid_t> single_src_vid = {0, 1, 2, 3, 4,
                                                        5, 6, 7, 8, 9};
static const std::vector<neug::vid_t> dst_vid = {3, 5, 7, 2, 1, 5, 9, 1, 0, 4};
static const std::vector<int32_t> int32_data = {1, 4, -1, 2, 9, 2, 4, 3, 1, -2};
static const std::vector<uint32_t> uint32_data = {0, 5, 2, 9, 3, 4, 3, 5, 7, 0};
static const std::vector<int64_t> int64_data = {1, 4, -1, 2, 9, 2, 4, 3, 1, -2};
static const std::vector<uint64_t> uint64_data = {0, 5, 2, 9, 3, 4, 3, 5, 7, 0};
static const std::vector<float> float_data = {1.0, 4.5,  -1.3, 2.2, 9.7,
                                              2.4, 4.12, 3.6,  1.8, -2.49};
static const std::vector<double> double_data = {1.0, 4.5,  -1.3, 2.2, 9.7,
                                                2.4, 4.12, 3.6,  1.8, -2.49};
static const std::vector<neug::Date> date_data = {
    neug::Date(0), neug::Date(5), neug::Date(2), neug::Date(9), neug::Date(3),
    neug::Date(4), neug::Date(3), neug::Date(5), neug::Date(7), neug::Date(0)};
static const std::vector<neug::DateTime> datetime_data = {
    neug::DateTime(0), neug::DateTime(5), neug::DateTime(2), neug::DateTime(9),
    neug::DateTime(3), neug::DateTime(4), neug::DateTime(3), neug::DateTime(5),
    neug::DateTime(7), neug::DateTime(0)};
static const std::vector<neug::Interval> interval_data = {
    neug::Interval(std::string("0hour")),
    neug::Interval(std::string("5hours")),
    neug::Interval(std::string("2minutes")),
    neug::Interval(std::string("9hours")),
    neug::Interval(std::string("3seconds")),
    neug::Interval(std::string("4days")),
    neug::Interval(std::string("3years")),
    neug::Interval(std::string("5milliseconds")),
    neug::Interval(std::string("7minutes")),
    neug::Interval(std::string("0day"))};
static const std::vector<neug::EmptyType> empty_data;

static const std::set<neug::vid_t> delete_src_vertices = {1, 4};
static const std::set<neug::vid_t> delete_dst_vertices = {1, 5, 7};
static const size_t enum_after_delete_vertex = 1;
static const size_t enum_after_delete_vertex_single = 5;

static const std::vector<neug::vid_t> delete_src_edges = {0, 4, 4};
static const std::vector<neug::vid_t> delete_dst_edges = {3, 4, 1};
static const size_t enum_after_delete_edge = 7;
static const size_t enum_after_delete_edge_single = 8;

static const neug::vid_t insert_src_vid = 1;
static const neug::vid_t insert_dst_vid = 4;

namespace neug {
namespace test {
using Datatypes =
    ::testing::Types<neug::EmptyType, int32_t, uint32_t, int64_t, uint64_t,
                     double, float, Date, DateTime, Interval>;

template <typename EDATA_T>
class MutableCsrTest : public ::testing::Test {
 protected:
  static constexpr const char* TEST_DIR = "/tmp/mutable_csr_test";

  void SetUp() override {
    allocators.resize(1, Allocator(neug::MemoryStrategy::kMemoryOnly, ""));
  }

  size_t count_edge_num(MutableCsr<EDATA_T>& csr) {
    size_t edge_num = 0;
    auto oe_view = csr.get_generic_view(0);
    for (neug::vid_t src = 0; src < src_v_num; ++src) {
      auto edges = oe_view.get_edges(src);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        edge_num += 1;
      }
    }
    return edge_num;
  }

  void load_csr_data(MutableCsr<EDATA_T>& csr, int memory_level) {
    if (std::filesystem::exists(TEST_DIR)) {
      std::filesystem::remove_all(TEST_DIR);
    }
    std::filesystem::create_directories(TEST_DIR);

    if (memory_level == 0) {
      csr.open("csr_data", "", TEST_DIR);
    } else if (memory_level == 1) {
      csr.open_in_memory("csr_data");
    }
    csr.resize(src_v_num);
    if constexpr (std::is_same_v<EDATA_T, int32_t>) {
      csr.batch_put_edges(src_vid, dst_vid, int32_data);
    } else if constexpr (std::is_same_v<EDATA_T, int64_t>) {
      csr.batch_put_edges(src_vid, dst_vid, int64_data);
    } else if constexpr (std::is_same_v<EDATA_T, uint32_t>) {
      csr.batch_put_edges(src_vid, dst_vid, uint32_data);
    } else if constexpr (std::is_same_v<EDATA_T, uint64_t>) {
      csr.batch_put_edges(src_vid, dst_vid, uint64_data);
    } else if constexpr (std::is_same_v<EDATA_T, float>) {
      csr.batch_put_edges(src_vid, dst_vid, float_data);
    } else if constexpr (std::is_same_v<EDATA_T, double>) {
      csr.batch_put_edges(src_vid, dst_vid, double_data);
    } else if constexpr (std::is_same_v<EDATA_T, Date>) {
      csr.batch_put_edges(src_vid, dst_vid, date_data);
    } else if constexpr (std::is_same_v<EDATA_T, DateTime>) {
      csr.batch_put_edges(src_vid, dst_vid, datetime_data);
    } else if constexpr (std::is_same_v<EDATA_T, Interval>) {
      csr.batch_put_edges(src_vid, dst_vid, interval_data);
    } else if constexpr (std::is_same_v<EDATA_T, neug::EmptyType>) {
      csr.batch_put_edges(src_vid, dst_vid, empty_data);
    } else {
      FAIL();
    }
  }

  void load_single_csr_data(SingleMutableCsr<EDATA_T>& csr, int memory_level) {
    if (std::filesystem::exists(TEST_DIR)) {
      std::filesystem::remove_all(TEST_DIR);
    }
    std::filesystem::create_directories(TEST_DIR);

    if (memory_level == 0) {
      csr.open("single_csr_data", "", TEST_DIR);
    } else if (memory_level == 1) {
      csr.open_in_memory("single_csr_data");
    }
    csr.resize(single_src_v_num);
    if constexpr (std::is_same_v<EDATA_T, int32_t>) {
      csr.batch_put_edges(single_src_vid, dst_vid, int32_data);
    } else if constexpr (std::is_same_v<EDATA_T, int64_t>) {
      csr.batch_put_edges(single_src_vid, dst_vid, int64_data);
    } else if constexpr (std::is_same_v<EDATA_T, uint32_t>) {
      csr.batch_put_edges(single_src_vid, dst_vid, uint32_data);
    } else if constexpr (std::is_same_v<EDATA_T, uint64_t>) {
      csr.batch_put_edges(single_src_vid, dst_vid, uint64_data);
    } else if constexpr (std::is_same_v<EDATA_T, float>) {
      csr.batch_put_edges(single_src_vid, dst_vid, float_data);
    } else if constexpr (std::is_same_v<EDATA_T, double>) {
      csr.batch_put_edges(single_src_vid, dst_vid, double_data);
    } else if constexpr (std::is_same_v<EDATA_T, Date>) {
      csr.batch_put_edges(single_src_vid, dst_vid, date_data);
    } else if constexpr (std::is_same_v<EDATA_T, DateTime>) {
      csr.batch_put_edges(single_src_vid, dst_vid, datetime_data);
    } else if constexpr (std::is_same_v<EDATA_T, Interval>) {
      csr.batch_put_edges(single_src_vid, dst_vid, interval_data);
    } else if constexpr (std::is_same_v<EDATA_T, neug::EmptyType>) {
      csr.batch_put_edges(single_src_vid, dst_vid, empty_data);
    } else {
      FAIL();
    }
  }

  bool check_edge_data_ordered(GenericView& generic_view) {
    for (vid_t v = 0; v < src_v_num; v++) {
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

  template <template <typename> class CSR_T>
  void put_single_edge(CSR_T<EDATA_T>& csr, neug::vid_t src, neug::vid_t dst,
                       timestamp_t ts, Allocator& allocator) {
    if constexpr (std::is_same_v<EDATA_T, int32_t>) {
      int32_t data = 100;
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, int64_t>) {
      int64_t data = 100;
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, uint32_t>) {
      uint32_t data = 100;
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, uint64_t>) {
      uint64_t data = 100;
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, float>) {
      float data = 100.0;
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, double>) {
      double data = 100.0;
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, Date>) {
      Date data(100);
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, DateTime>) {
      DateTime data(100);
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, Interval>) {
      Interval data(std::string("100seconds"));
      csr.put_edge(src, dst, data, ts, allocator);
    } else if constexpr (std::is_same_v<EDATA_T, neug::EmptyType>) {
      neug::EmptyType data;
      csr.put_edge(src, dst, data, ts, allocator);
    } else {
      FAIL();
    }
  }

  std::vector<neug::Allocator> allocators;
};
TYPED_TEST_SUITE(MutableCsrTest, Datatypes);

TYPED_TEST(MutableCsrTest, TestCsrType) {
  MutableCsr<TypeParam> mutable_csr;
  EXPECT_EQ(mutable_csr.csr_type(), CsrType::kMutable);
  SingleMutableCsr<TypeParam> single_mutable_csr;
  EXPECT_EQ(single_mutable_csr.csr_type(), CsrType::kSingleMutable);
  EmptyCsr<TypeParam> empty_csr;
  EXPECT_EQ(empty_csr.csr_type(), CsrType::kEmpty);
}

TYPED_TEST(MutableCsrTest, TestUnsortedSince) {
  timestamp_t sort_ts = 10;
  MutableCsr<TypeParam> mutable_csr;
  mutable_csr.batch_sort_by_edge_data(10);
  EXPECT_EQ(mutable_csr.unsorted_since(), sort_ts);
  SingleMutableCsr<TypeParam> single_mutable_csr;
  single_mutable_csr.batch_sort_by_edge_data(10);
  EXPECT_EQ(single_mutable_csr.unsorted_since(),
            std::numeric_limits<timestamp_t>::max());
  EmptyCsr<TypeParam> empty_csr;
  empty_csr.batch_sort_by_edge_data(10);
  EXPECT_EQ(empty_csr.unsorted_since(),
            std::numeric_limits<timestamp_t>::max());
}

TYPED_TEST(MutableCsrTest, TestBasicFunction) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  EXPECT_EQ(mutable_csr.size(), src_v_num);
  EXPECT_EQ(mutable_csr.edge_num(), edge_num);
  mutable_csr.compact();
  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  EXPECT_EQ(single_mutable_csr.size(), single_src_v_num);
  EXPECT_EQ(single_mutable_csr.edge_num(), edge_num);
  single_mutable_csr.compact();
  EmptyCsr<TypeParam> empty_csr;
  EXPECT_EQ(empty_csr.size(), 0);
  EXPECT_EQ(empty_csr.edge_num(), 0);
  empty_csr.compact();
  empty_csr.close();
}

TYPED_TEST(MutableCsrTest, TestDumpAndOpen) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  mutable_csr.dump("dumped_csr_data", this->TEST_DIR);
  std::filesystem::create_directories(tmp_dir(this->TEST_DIR));
  MutableCsr<TypeParam> fmap_mutable_csr, memory_mutable_csr,
      hugepage_mutable_csr;
  fmap_mutable_csr.open("dumped_csr_data", this->TEST_DIR, this->TEST_DIR);
  EXPECT_EQ(fmap_mutable_csr.edge_num(), edge_num);
  memory_mutable_csr.open_in_memory(std::string(this->TEST_DIR) +
                                    "/dumped_csr_data");
  EXPECT_EQ(memory_mutable_csr.edge_num(), edge_num);
  hugepage_mutable_csr.open_with_hugepages(std::string(this->TEST_DIR) +
                                           "/dumped_csr_data");
  EXPECT_EQ(hugepage_mutable_csr.edge_num(), edge_num);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  single_mutable_csr.dump("dumped_csr_data", this->TEST_DIR);
  std::filesystem::create_directories(tmp_dir(this->TEST_DIR));
  SingleMutableCsr<TypeParam> fmap_single_mutable_csr,
      memory_single_mutable_csr, hugepage_single_mutable_csr;
  fmap_single_mutable_csr.open("dumped_csr_data", this->TEST_DIR,
                               this->TEST_DIR);
  EXPECT_EQ(fmap_single_mutable_csr.edge_num(), edge_num);
  memory_single_mutable_csr.open_in_memory(std::string(this->TEST_DIR) +
                                           "/dumped_csr_data");
  EXPECT_EQ(memory_single_mutable_csr.edge_num(), edge_num);
  hugepage_single_mutable_csr.open_with_hugepages(std::string(this->TEST_DIR) +
                                                  "/dumped_csr_data");
  EXPECT_EQ(hugepage_single_mutable_csr.edge_num(), edge_num);

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.dump("dumped_csr_data", this->TEST_DIR);
  EmptyCsr<TypeParam> opened_empty_csr;
  opened_empty_csr.open("dumped_csr_data", this->TEST_DIR, this->TEST_DIR);
  opened_empty_csr.open_in_memory(std::string(this->TEST_DIR) +
                                  "/dumped_csr_data");
  opened_empty_csr.open_with_hugepages(std::string(this->TEST_DIR) +
                                       "/dumped_csr_data");
}

TYPED_TEST(MutableCsrTest, TestResize) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  mutable_csr.resize(src_v_num - 2);
  EXPECT_EQ(mutable_csr.size(), src_v_num - 2);
  mutable_csr.close();

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  single_mutable_csr.resize(single_src_v_num - 2);
  EXPECT_EQ(single_mutable_csr.size(), single_src_v_num - 2);
  single_mutable_csr.close();

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.resize(single_src_v_num);
  EXPECT_EQ(empty_csr.size(), 0);
}

TYPED_TEST(MutableCsrTest, TestSortByEdgeData) {
  timestamp_t sort_ts = 10;

  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  mutable_csr.batch_sort_by_edge_data(sort_ts);
  GenericView mutable_view = mutable_csr.get_generic_view(sort_ts);
  EXPECT_EQ(mutable_view.type(), CsrViewType::kMultipleMutable);
  EXPECT_TRUE(this->check_edge_data_ordered(mutable_view));
  mutable_csr.close();

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  single_mutable_csr.batch_sort_by_edge_data(sort_ts);
  GenericView single_mutable_view =
      single_mutable_csr.get_generic_view(sort_ts);
  EXPECT_EQ(single_mutable_view.type(), CsrViewType::kSingleMutable);
  single_mutable_csr.close();

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.batch_sort_by_edge_data(sort_ts);
}

TYPED_TEST(MutableCsrTest, TestBatchDeleteVertices) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  mutable_csr.batch_delete_vertices(delete_src_vertices, delete_dst_vertices);
  EXPECT_EQ(mutable_csr.edge_num(), enum_after_delete_vertex);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  single_mutable_csr.batch_delete_vertices(delete_src_vertices,
                                           delete_dst_vertices);
  EXPECT_EQ(single_mutable_csr.edge_num(), enum_after_delete_vertex_single);

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.batch_delete_vertices(delete_src_vertices, delete_dst_vertices);
}

TYPED_TEST(MutableCsrTest, TestBatchDeleteEdges) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  std::vector<std::pair<vid_t, int32_t>> edges_to_delete;
  auto view = mutable_csr.get_generic_view(1);
  for (size_t i = 0; i < delete_src_edges.size(); ++i) {
    auto edges = view.get_edges(delete_src_edges[i]);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (it.get_vertex() == delete_dst_edges[i]) {
        auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                       reinterpret_cast<const char*>(edges.start_ptr)) /
                      it.cfg.stride;
        edges_to_delete.emplace_back(delete_src_edges[i],
                                     static_cast<int32_t>(offset));
        break;
      }
    }
  }
  mutable_csr.batch_delete_edges(edges_to_delete);
  EXPECT_EQ(mutable_csr.edge_num(), enum_after_delete_edge);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  edges_to_delete.clear();
  view = single_mutable_csr.get_generic_view(1);
  for (size_t i = 0; i < delete_src_edges.size(); ++i) {
    auto edges = view.get_edges(delete_src_edges[i]);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (it.get_vertex() == delete_dst_edges[i]) {
        auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                       reinterpret_cast<const char*>(edges.start_ptr)) /
                      it.cfg.stride;
        edges_to_delete.emplace_back(delete_src_edges[i], offset);
        assert(offset == 0);
        break;
      }
    }
  }

  single_mutable_csr.batch_delete_edges(edges_to_delete);
  EXPECT_EQ(single_mutable_csr.edge_num(), enum_after_delete_edge_single);

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.batch_delete_edges(edges_to_delete);
}

TYPED_TEST(MutableCsrTest, TestBatchDeleteEdgesById) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  std::vector<vid_t> src_ids, dst_ids;
  auto view = mutable_csr.get_generic_view(1);
  for (vid_t i = 0; i < mutable_csr.size(); i++) {
    auto edges = view.get_edges(i);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      src_ids.emplace_back(i);
      dst_ids.emplace_back(it.get_vertex());
    }
    if (src_ids.size() > 4) {
      break;
    }
  }
  mutable_csr.batch_delete_edges(src_ids, dst_ids);
  EXPECT_EQ(mutable_csr.edge_num(), 10 - src_ids.size());

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  src_ids.clear();
  dst_ids.clear();
  view = single_mutable_csr.get_generic_view(1);
  for (size_t i = 0; i < 4; ++i) {
    auto edges = view.get_edges(i);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      src_ids.emplace_back(i);
      dst_ids.emplace_back(it.get_vertex());
      break;
    }
  }

  single_mutable_csr.batch_delete_edges(src_ids, dst_ids);
  EXPECT_EQ(single_mutable_csr.edge_num(), 6);
}

TYPED_TEST(MutableCsrTest, TestPutEdge) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  std::set<vid_t> delete_src_id = {insert_src_vid};
  std::set<vid_t> empty_set;
  single_mutable_csr.batch_delete_vertices(delete_src_id, empty_set);

  EmptyCsr<TypeParam> empty_csr;
  timestamp_t insert_ts = 6;
  this->template put_single_edge<MutableCsr>(mutable_csr, insert_src_vid,
                                             insert_dst_vid, insert_ts,
                                             this->allocators[0]);
  this->template put_single_edge<SingleMutableCsr>(
      single_mutable_csr, insert_src_vid, insert_dst_vid, insert_ts,
      this->allocators[0]);
  this->template put_single_edge<EmptyCsr>(empty_csr, insert_src_vid,
                                           insert_dst_vid, insert_ts,
                                           this->allocators[0]);
  EXPECT_EQ(mutable_csr.edge_num(), edge_num + 1);
  EXPECT_EQ(single_mutable_csr.edge_num(), edge_num);
  EXPECT_EQ(empty_csr.edge_num(), 0);

  mutable_csr.reset_timestamp();
  single_mutable_csr.reset_timestamp();
  empty_csr.reset_timestamp();
}

TEST(CsrToolTest, OpenNonExistFile) {
  void* buffer;
  std::string non_exist_filename = "/invalid/non_exist_file";
  EXPECT_THROW(read_file(non_exist_filename, buffer, 0, 0),
               neug::exception::Exception);
}

TYPED_TEST(MutableCsrTest, TestDeleteEdge) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, 1);
  std::vector<std::tuple<vid_t, vid_t, int32_t>> edges_to_delete;
  auto oe_view = mutable_csr.get_generic_view(0);
  std::set<size_t> deleted_indices;
  while (edges_to_delete.size() < 4) {
    size_t idx;
    while (true) {
      idx = std::rand() % edge_num;
      if (deleted_indices.find(idx) == deleted_indices.end()) {
        deleted_indices.insert(idx);
        break;
      }
    }
    auto edges = oe_view.get_edges(src_vid[idx]);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (it.get_vertex() == dst_vid[idx]) {
        auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                       reinterpret_cast<const char*>(edges.start_ptr)) /
                      it.cfg.stride;
        edges_to_delete.emplace_back(src_vid[idx], dst_vid[idx],
                                     static_cast<int32_t>(offset));
        break;
      }
    }
  }

  for (const auto& edge : edges_to_delete) {
    mutable_csr.delete_edge(std::get<0>(edge), std::get<2>(edge), 0);
  }

  EXPECT_EQ(this->count_edge_num(mutable_csr),
            edge_num - edges_to_delete.size());

  for (const auto& edge : edges_to_delete) {
    mutable_csr.revert_delete_edge(std::get<0>(edge), std::get<1>(edge),
                                   std::get<2>(edge), 0);
  }

  EXPECT_EQ(this->count_edge_num(mutable_csr), edge_num);

  // Try to revert deletion again, which should fail.
  for (const auto& edge : edges_to_delete) {
    EXPECT_THROW(
        mutable_csr.revert_delete_edge(std::get<0>(edge), std::get<1>(edge),
                                       std::get<2>(edge), 0),
        neug::exception::Exception);
  }

  for (size_t i = 0; i < 50; ++i) {
    for (size_t src_ind = 0; src_ind < src_vid.size(); ++src_ind) {
      this->template put_single_edge<MutableCsr>(mutable_csr, src_vid[src_ind],
                                                 dst_vid[src_ind], 0,
                                                 this->allocators[0]);
    }
  }
  EXPECT_EQ(this->count_edge_num(mutable_csr), edge_num + 50 * src_vid.size());

  mutable_csr.close();

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, 1);
  size_t edge_num = single_mutable_csr.edge_num();
  edges_to_delete.clear();
  oe_view = single_mutable_csr.get_generic_view(0);
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
    single_mutable_csr.delete_edge(std::get<0>(edge), std::get<2>(edge), 0);
  }
  EXPECT_EQ(single_mutable_csr.edge_num(), edge_num - edges_to_delete.size());
  for (const auto& edge : edges_to_delete) {
    single_mutable_csr.revert_delete_edge(std::get<0>(edge), std::get<1>(edge),
                                          std::get<2>(edge), 0);
  }

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.delete_edge(0, 0, 0);
  empty_csr.revert_delete_edge(0, 0, 0, 0);
}
}  // namespace test
}  // namespace neug