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
#include <sys/stat.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/csr/mutable_csr.h"
#include "unittest/utils.h"

namespace {

// Creates a unique temporary directory for a single test instance.
// Uses the system temp dir + a PID-based prefix + random suffix to avoid
// collisions under parallel test execution or concurrent CI jobs.
std::filesystem::path make_unique_test_dir(const std::string& test_name) {
  std::mt19937_64 rng(
      std::chrono::steady_clock::now().time_since_epoch().count() ^
      static_cast<uint64_t>(::getpid()));
  std::uniform_int_distribution<uint64_t> dist;
  auto unique_dir = std::filesystem::temp_directory_path() /
                    (test_name + "_" + std::to_string(::getpid()) + "_" +
                     std::to_string(dist(rng)));
  std::filesystem::create_directories(unique_dir);
  return unique_dir;
}

}  // namespace

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

namespace {

struct CsrCowSignature {
  size_t edge_num{0};
  size_t src0_degree{0};
  int64_t dst_sum{0};
  int64_t data_sum{0};
};

template <typename CSR_T>
CsrCowSignature build_cow_signature(const CSR_T& csr) {
  CsrCowSignature sig;
  sig.edge_num = csr.edge_num();
  auto view = csr.get_generic_view(0);
  for (vid_t src = 0; src < csr.size(); ++src) {
    auto edges = view.get_edges(src);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (src == 0) {
        ++sig.src0_degree;
      }
      sig.dst_sum += it.get_vertex();
      sig.data_sum += *static_cast<const int32_t*>(it.get_data_ptr());
    }
  }
  return sig;
}

template <typename CSR_T>
std::tuple<vid_t, vid_t, int32_t> find_first_edge(const CSR_T& csr) {
  auto view = csr.get_generic_view(0);
  for (vid_t src = 0; src < csr.size(); ++src) {
    auto edges = view.get_edges(src);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      auto offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                     reinterpret_cast<const char*>(edges.start_ptr)) /
                    it.cfg.stride;
      return {src, it.get_vertex(), static_cast<int32_t>(offset)};
    }
  }
  return {std::numeric_limits<vid_t>::max(), std::numeric_limits<vid_t>::max(),
          -1};
}

template <typename CSR_T>
void apply_cow_mutations(CSR_T& csr, Allocator& alloc) {
  csr.DetachVertex(0, alloc);
  // csr.batch_put_edges({0}, {1}, {111}, 0);
  csr.put_edge(0, 0, 111, 0, alloc);

  auto [src, dst, offset] = find_first_edge(csr);
  ASSERT_NE(offset, -1);
  csr.DetachVertex(src, alloc);
  csr.delete_edge(src, offset, 0);
  csr.revert_delete_edge(src, dst, offset, 0);

  csr.DetachVertex(2, alloc);
  // csr.batch_put_edges({2}, {3}, {222}, 0);
  csr.put_edge(2, 3, 222, 0, alloc);
}

void expect_signature_eq(const CsrCowSignature& lhs,
                         const CsrCowSignature& rhs) {
  EXPECT_EQ(lhs.edge_num, rhs.edge_num);
  EXPECT_EQ(lhs.src0_degree, rhs.src0_degree);
  EXPECT_EQ(lhs.dst_sum, rhs.dst_sum);
  EXPECT_EQ(lhs.data_sum, rhs.data_sum);
}

template <MemoryLevel OPEN_LEVEL, MemoryLevel MATERIALIZE_LEVEL>
struct CsrMaterializeLevelCase {
  static constexpr MemoryLevel kOpenLevel = OPEN_LEVEL;
  static constexpr MemoryLevel kMaterializeLevel = MATERIALIZE_LEVEL;
};

using MutableCsrMaterializeLevelCases = ::testing::Types<
    CsrMaterializeLevelCase<MemoryLevel::kInMemory, MemoryLevel::kInMemory>,
    CsrMaterializeLevelCase<MemoryLevel::kInMemory,
                            MemoryLevel::kHugePagePreferred>,
    CsrMaterializeLevelCase<MemoryLevel::kInMemory, MemoryLevel::kSyncToFile>,
    CsrMaterializeLevelCase<MemoryLevel::kHugePagePreferred,
                            MemoryLevel::kInMemory>,
    CsrMaterializeLevelCase<MemoryLevel::kHugePagePreferred,
                            MemoryLevel::kHugePagePreferred>,
    CsrMaterializeLevelCase<MemoryLevel::kHugePagePreferred,
                            MemoryLevel::kSyncToFile>,
    CsrMaterializeLevelCase<MemoryLevel::kSyncToFile, MemoryLevel::kInMemory>,
    CsrMaterializeLevelCase<MemoryLevel::kSyncToFile,
                            MemoryLevel::kHugePagePreferred>,
    CsrMaterializeLevelCase<MemoryLevel::kSyncToFile,
                            MemoryLevel::kSyncToFile>>;

template <typename CASE_T>
class MutableCsrCowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("mutable_csr_cow_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()) +
         "_" + GetTestName());
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
    std::filesystem::create_directories(temp_dir_);
    checkpoint_mgr_.Open(temp_dir_.string());
  }

  void TearDown() override {
    if (std::filesystem::exists(temp_dir_)) {
      std::filesystem::remove_all(temp_dir_);
    }
  }

  std::shared_ptr<Checkpoint> create_checkpoint() {
    return make_checkpoint(checkpoint_mgr_);
  }

 private:
  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }

 protected:
  CheckpointManager checkpoint_mgr_;
  std::filesystem::path temp_dir_;
};

TYPED_TEST_SUITE(MutableCsrCowTest, MutableCsrMaterializeLevelCases);

TYPED_TEST(MutableCsrCowTest, CowIsolationAndDumpOpenMatrix) {
  MutableCsr<int32_t> original;
  auto base_ckp = this->create_checkpoint();
  original.Open(*base_ckp, ModuleDescriptor(), TypeParam::kOpenLevel);
  original.resize(src_v_num);
  original.batch_put_edges(src_vid, dst_vid, int32_data, 0);

  auto original_before = build_cow_signature(original);

  auto cow_module = original.Clone();
  auto* cow = dynamic_cast<MutableCsr<int32_t>*>(cow_module.get());
  ASSERT_NE(cow, nullptr);
  // Detach detaches IDataContainer so writes to cow don't affect
  // original.
  cow->Detach(*base_ckp, TypeParam::kMaterializeLevel);
  Allocator alloc(MemoryLevel::kInMemory, "");

  apply_cow_mutations(*cow, alloc);
  auto cow_after = build_cow_signature(*cow);

  auto original_after_cow_mutation = build_cow_signature(original);
  expect_signature_eq(original_after_cow_mutation, original_before);

  apply_cow_mutations(original, alloc);
  auto original_after_self_mutation = build_cow_signature(original);
  EXPECT_NE(original_after_self_mutation.edge_num, original_before.edge_num);

  auto cow_after_original_mutation = build_cow_signature(*cow);
  expect_signature_eq(cow_after_original_mutation, cow_after);

  auto dump_ckp = this->create_checkpoint();
  auto cow_desc = dump_module_descriptor(*cow, *dump_ckp, "cow_csr");
  MutableCsr<int32_t> reopened;
  reopened.Open(*dump_ckp, cow_desc, MemoryLevel::kInMemory);
  auto reopened_sig = build_cow_signature(reopened);
  expect_signature_eq(reopened_sig, cow_after);
}

}  // namespace

template <typename EDATA_T>
class MutableCsrTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = make_unique_test_dir("mutable_csr_test");
    allocators.emplace_back(
        std::make_unique<Allocator>(MemoryLevel::kInMemory, ""));
    checkpoint_mgr_.Open(test_dir_.string());
  }

  void TearDown() override {
    checkpoint_mgr_.Close();
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
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

  std::shared_ptr<Checkpoint> load_csr_data(MutableCsr<EDATA_T>& csr,
                                            MemoryLevel memory_level) {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);
    // The previous helper invocation left checkpoints in checkpoint_mgr_; the
    // directory wipe above made them stale, so re-sync the workspace with disk
    // before creating a new checkpoint.
    checkpoint_mgr_.Close();
    checkpoint_mgr_.Open(test_dir_.string());

    auto ckp = make_checkpoint(checkpoint_mgr_);
    csr.Open(*ckp, ModuleDescriptor(), memory_level);
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
      ADD_FAILURE() << "Unsupported data type";
    }
    return ckp;
  }

  std::shared_ptr<Checkpoint> load_single_csr_data(
      SingleMutableCsr<EDATA_T>& csr, MemoryLevel memory_level) {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);
    // The previous helper invocation left checkpoints in checkpoint_mgr_; the
    // directory wipe above made them stale, so re-sync the workspace with disk
    // before creating a new checkpoint.
    checkpoint_mgr_.Close();
    checkpoint_mgr_.Open(test_dir_.string());

    auto ckp = make_checkpoint(checkpoint_mgr_);
    csr.Open(*ckp, ModuleDescriptor(), memory_level);
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
      ADD_FAILURE() << "Unsupported data type";
    }
    return ckp;
  }

  bool check_edge_data_ordered(CsrView& generic_view) {
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

  std::vector<std::unique_ptr<neug::Allocator>> allocators;
  CheckpointManager& workspace() { return checkpoint_mgr_; }
  std::filesystem::path test_dir_;
  CheckpointManager checkpoint_mgr_;
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
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
  EXPECT_EQ(mutable_csr.size(), src_v_num);
  EXPECT_EQ(mutable_csr.edge_num(), edge_num);
  mutable_csr.compact();
  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
  EXPECT_EQ(single_mutable_csr.size(), single_src_v_num);
  EXPECT_EQ(single_mutable_csr.edge_num(), edge_num);
  single_mutable_csr.compact();
  EmptyCsr<TypeParam> empty_csr;
  EXPECT_EQ(empty_csr.size(), 0);
  EXPECT_EQ(empty_csr.edge_num(), 0);
  empty_csr.compact();
}

TYPED_TEST(MutableCsrTest, TestDumpAndOpen) {
  MutableCsr<TypeParam> mutable_csr;
  auto ckp = this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
  auto desc = dump_module_descriptor(mutable_csr, *ckp, "mutable_csr");
  MutableCsr<TypeParam> fmap_mutable_csr, memory_mutable_csr,
      hugepage_mutable_csr;
  fmap_mutable_csr.Open(*ckp, desc, MemoryLevel::kSyncToFile);
  EXPECT_EQ(fmap_mutable_csr.edge_num(), edge_num);
  memory_mutable_csr.Open(*ckp, desc, MemoryLevel::kInMemory);
  EXPECT_EQ(memory_mutable_csr.edge_num(), edge_num);
  hugepage_mutable_csr.Open(*ckp, desc, MemoryLevel::kHugePagePreferred);
  EXPECT_EQ(hugepage_mutable_csr.edge_num(), edge_num);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  auto single_ckp =
      this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
  auto single_desc = dump_module_descriptor(single_mutable_csr, *single_ckp,
                                            "single_mutable_csr");
  SingleMutableCsr<TypeParam> fmap_single_mutable_csr,
      memory_single_mutable_csr, hugepage_single_mutable_csr;
  fmap_single_mutable_csr.Open(*single_ckp, single_desc,
                               MemoryLevel::kSyncToFile);
  EXPECT_EQ(fmap_single_mutable_csr.edge_num(), edge_num);
  memory_single_mutable_csr.Open(*single_ckp, single_desc,
                                 MemoryLevel::kInMemory);
  EXPECT_EQ(memory_single_mutable_csr.edge_num(), edge_num);
  hugepage_single_mutable_csr.Open(*single_ckp, single_desc,
                                   MemoryLevel::kHugePagePreferred);
  EXPECT_EQ(hugepage_single_mutable_csr.edge_num(), edge_num);

  EmptyCsr<TypeParam> empty_csr;
  auto empty_desc = dump_module_descriptor(empty_csr, *single_ckp, "empty_csr");
  EmptyCsr<TypeParam> opened_empty_csr;
  opened_empty_csr.Open(*single_ckp, empty_desc, MemoryLevel::kSyncToFile);
  opened_empty_csr.Open(*single_ckp, empty_desc, MemoryLevel::kInMemory);
  opened_empty_csr.Open(*single_ckp, empty_desc,
                        MemoryLevel::kHugePagePreferred);
}

TYPED_TEST(MutableCsrTest, TestResize) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
  mutable_csr.resize(src_v_num - 2);
  EXPECT_EQ(mutable_csr.size(), src_v_num - 2);
  mutable_csr.Close();

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
  single_mutable_csr.resize(single_src_v_num - 2);
  EXPECT_EQ(single_mutable_csr.size(), single_src_v_num - 2);
  single_mutable_csr.Close();

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.resize(single_src_v_num);
  EXPECT_EQ(empty_csr.size(), 0);
}

TYPED_TEST(MutableCsrTest, TestSortByEdgeData) {
  timestamp_t sort_ts = 10;

  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
  mutable_csr.batch_sort_by_edge_data(sort_ts);
  CsrView mutable_view = mutable_csr.get_generic_view(sort_ts);
  EXPECT_EQ(mutable_view.type(), CsrViewType::kMultipleMutable);
  EXPECT_TRUE(this->check_edge_data_ordered(mutable_view));
  mutable_csr.Close();

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
  single_mutable_csr.batch_sort_by_edge_data(sort_ts);
  CsrView single_mutable_view = single_mutable_csr.get_generic_view(sort_ts);
  EXPECT_EQ(single_mutable_view.type(), CsrViewType::kSingleMutable);
  single_mutable_csr.Close();

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.batch_sort_by_edge_data(sort_ts);
}

TYPED_TEST(MutableCsrTest, TestBatchDeleteVertices) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
  mutable_csr.batch_delete_vertices(delete_src_vertices, delete_dst_vertices);
  EXPECT_EQ(mutable_csr.edge_num(), enum_after_delete_vertex);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
  single_mutable_csr.batch_delete_vertices(delete_src_vertices,
                                           delete_dst_vertices);
  EXPECT_EQ(single_mutable_csr.edge_num(), enum_after_delete_vertex_single);

  EmptyCsr<TypeParam> empty_csr;
  empty_csr.batch_delete_vertices(delete_src_vertices, delete_dst_vertices);
}

TYPED_TEST(MutableCsrTest, TestBatchDeleteEdges) {
  MutableCsr<TypeParam> mutable_csr;
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
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
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
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
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
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
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
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
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
  std::set<vid_t> delete_src_id = {insert_src_vid};
  std::set<vid_t> empty_set;
  single_mutable_csr.batch_delete_vertices(delete_src_id, empty_set);

  EmptyCsr<TypeParam> empty_csr;
  timestamp_t insert_ts = 6;
  this->template put_single_edge<MutableCsr>(mutable_csr, insert_src_vid,
                                             insert_dst_vid, insert_ts,
                                             *(this->allocators[0]));
  this->template put_single_edge<SingleMutableCsr>(
      single_mutable_csr, insert_src_vid, insert_dst_vid, insert_ts,
      *(this->allocators[0]));
  this->template put_single_edge<EmptyCsr>(empty_csr, insert_src_vid,
                                           insert_dst_vid, insert_ts,
                                           *(this->allocators[0]));
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
  this->load_csr_data(mutable_csr, MemoryLevel::kInMemory);
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
                                                 *(this->allocators[0]));
    }
  }
  EXPECT_EQ(this->count_edge_num(mutable_csr), edge_num + 50 * src_vid.size());

  mutable_csr.Close();

  SingleMutableCsr<TypeParam> single_mutable_csr;
  this->load_single_csr_data(single_mutable_csr, MemoryLevel::kInMemory);
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
// ---------------------------------------------------------------------------
// DumpDirty: Validate fast vs. slow path of Dump() and that mutations mark the
// CSR dirty as expected.
// ---------------------------------------------------------------------------
class MutableCsrDumpDirtyTest : public ::testing::Test {
 protected:
  using CsrT = MutableCsr<int64_t>;
  static constexpr vid_t VNUM = 5;
  const std::vector<vid_t> src_ = {0, 0, 1, 2, 4};
  const std::vector<vid_t> dst_ = {3, 4, 2, 1, 0};
  const std::vector<int64_t> data_ = {10, 20, 30, 40, 50};

  void SetUp() override {
    test_dir_ = make_unique_test_dir("mutable_csr_dump_dirty_test");
    checkpoint_mgr_.Open(test_dir_.string());
    alloc_ = std::make_unique<Allocator>(MemoryLevel::kInMemory, "");
  }
  void TearDown() override {
    checkpoint_mgr_.Close();
    if (std::filesystem::exists(test_dir_))
      std::filesystem::remove_all(test_dir_);
  }

  std::shared_ptr<Checkpoint> prepare(CsrT& csr, ModuleDescriptor& desc) {
    CsrT orig;
    auto ckp = make_checkpoint(checkpoint_mgr_);
    orig.Open(*ckp, ModuleDescriptor(), MemoryLevel::kInMemory);
    orig.resize(VNUM);
    orig.batch_put_edges(src_, dst_, data_);
    desc = dump_module_descriptor(orig, *ckp, "orig_csr");
    csr.Open(*ckp, desc, MemoryLevel::kInMemory);
    return ckp;
  }

  // Returns the inode number of the file at `path`.
  // Throws std::runtime_error on stat() failure so that any follow-on
  // inode comparisons are never made against an uninitialized value.
  static ino_t inode_of(const std::string& path) {
    struct stat st {};
    if (stat(path.c_str(), &st) != 0) {
      throw std::runtime_error("stat() failed for path: " + path + " — " +
                               std::strerror(errno));
    }
    return st.st_ino;
  }
  std::string nbr_path(const ModuleDescriptor& d) {
    return d.get_path(ModuleDescriptor::kNbrListPath).value();
  }
  void expect_slow_after(const std::string& label,
                         std::function<void(CsrT&)> mutate) {
    CsrT csr;
    ModuleDescriptor orig_desc;
    auto ckp = prepare(csr, orig_desc);
    auto orig_inode = inode_of(nbr_path(orig_desc));
    mutate(csr);
    auto new_desc = dump_module_descriptor(csr, *ckp, "csr");
    auto new_inode = inode_of(nbr_path(new_desc));
    EXPECT_NE(orig_inode, new_inode) << label << " should mark dirty";
  }

  std::filesystem::path test_dir_;
  CheckpointManager checkpoint_mgr_;
  std::unique_ptr<Allocator> alloc_;
};

TEST_F(MutableCsrDumpDirtyTest, FastAndSlowPath) {
  CsrT csr;
  ModuleDescriptor orig_desc;
  auto ckp = prepare(csr, orig_desc);
  auto orig = inode_of(nbr_path(orig_desc));
  auto fast_desc = dump_module_descriptor(csr, *ckp, "csr");
  EXPECT_EQ(orig, inode_of(nbr_path(fast_desc)));
  int64_t val = 999;
  csr.put_edge(0, 2, val, 1, *alloc_);
  auto slow_desc = dump_module_descriptor(csr, *ckp, "csr");
  EXPECT_NE(orig, inode_of(nbr_path(slow_desc)));
}

TEST_F(MutableCsrDumpDirtyTest, FastPath_DataIntegrity) {
  CsrT csr;
  ModuleDescriptor desc;
  auto ckp = prepare(csr, desc);
  auto fast_desc = dump_module_descriptor(csr, *ckp, "csr");
  CsrT restored;
  restored.Open(*ckp, fast_desc, MemoryLevel::kInMemory);
  EXPECT_EQ(restored.edge_num(), src_.size());
}

TEST_F(MutableCsrDumpDirtyTest, DirtyResetAcrossCheckpointCycles) {
  CsrT orig;
  auto ckp = make_checkpoint(checkpoint_mgr_);
  orig.Open(*ckp, ModuleDescriptor(), MemoryLevel::kInMemory);
  orig.resize(VNUM);
  orig.batch_put_edges(src_, dst_, data_);
  auto d1 = dump_module_descriptor(orig, *ckp, "orig_csr");
  auto p1 = nbr_path(d1);

  CsrT c2;
  c2.Open(*ckp, d1, MemoryLevel::kInMemory);
  auto d2 = dump_module_descriptor(c2, *ckp, "c2");
  auto p2 = nbr_path(d2);
  EXPECT_EQ(inode_of(p1), inode_of(p2));

  c2.reset_timestamp();
  auto d3 = dump_module_descriptor(c2, *ckp, "c2");
  auto p3 = nbr_path(d3);
  EXPECT_EQ(inode_of(p2), inode_of(p3));

  CsrT c3;
  c3.Open(*ckp, d3, MemoryLevel::kInMemory);
  auto d4 = dump_module_descriptor(c3, *ckp, "c3");
  auto p4 = nbr_path(d4);
  EXPECT_EQ(inode_of(p3), inode_of(p4));
}

TEST_F(MutableCsrDumpDirtyTest, VariousMutationsSetDirty) {
  expect_slow_after("delete_edge", [](CsrT& c) { c.delete_edge(0, 0, 1); });
  expect_slow_after("batch_put_edges",
                    [](CsrT& c) { c.batch_put_edges({0}, {1}, {123}); });
  expect_slow_after(
      "put_edge", [this](CsrT& c) { c.put_edge(0, 1, 123, 1, *this->alloc_); });
  expect_slow_after("batch_delete_edges",
                    [](CsrT& c) { c.batch_delete_edges({0}, {3}); });
}
// Concurrent read-write test: verifies that lock-free readers using
// get_edges() / foreach_nbr_lt() see consistent (degree, buffer) snapshots
// even when a concurrent writer triggers CSR buffer reallocation via put_edge.
// This is a regression test for the torn-read bug fixed by using
// std::atomic_ref in put_edge / GenericView / TypedView.
// ---------------------------------------------------------------------------

TYPED_TEST(MutableCsrTest, TestConcurrentPutEdgeAndRead) {
  // Use int64_t for the typed view test (foreach_nbr_lt requires ordered data)
  MutableCsr<TypeParam> csr;
  this->load_csr_data(csr, MemoryLevel::kInMemory);

  // Sort edges so that foreach_nbr_lt can use binary search path
  csr.batch_sort_by_edge_data(1);

  constexpr int kWriterIterations = 500;
  constexpr int kReaderIterations = 2000;
  constexpr int kNumReaders = 4;

  std::atomic<bool> stop_flag{false};
  std::atomic<int> reader_errors{0};

  // Writer thread: continuously inserts edges to vertex 0, which will trigger
  // multiple buffer reallocations (initial cap=8, grows by 1.5x each time).
  std::thread writer([&]() {
    for (int i = 0; i < kWriterIterations; ++i) {
      vid_t dst = static_cast<vid_t>(i % 100);
      timestamp_t ts = 2;  // visible to readers with ts >= 2
      this->template put_single_edge<MutableCsr>(csr, 0, dst, ts,
                                                 *(this->allocators[0]));
    }
    stop_flag.store(true, std::memory_order_release);
  });

  // Reader threads: continuously read edges via get_edges() and verify
  // that the returned NbrList has consistent start/end pointers.
  std::vector<std::thread> readers;
  for (int r = 0; r < kNumReaders; ++r) {
    readers.emplace_back([&, r]() {
      int local_errors = 0;
      for (int i = 0; i < kReaderIterations; ++i) {
        // Test GenericView::get_edges
        auto view = csr.get_generic_view(2);
        auto edges = view.get_edges(0);

        // Sanity check: end_ptr must be >= start_ptr
        if (edges.end_ptr < edges.start_ptr) {
          local_errors++;
          continue;
        }

        // Verify that iteration doesn't crash (the original bug caused SIGSEGV
        // here due to ptr/end being in different buffers)
        size_t count = 0;
        for (auto it = edges.begin(); it != edges.end(); ++it) {
          ++count;
          // Safety bound to prevent infinite loop in case of regression
          if (count > 100000) {
            local_errors++;
            break;
          }
        }

        // Test TypedView if data type supports comparison (foreach_nbr_lt)
        if constexpr (!std::is_same_v<TypeParam, EmptyType> &&
                      !std::is_same_v<TypeParam, Interval>) {
          auto typed_view =
              view.template get_typed_view<TypeParam,
                                           CsrViewType::kMultipleMutable>();
          size_t lt_count = 0;
          typed_view.foreach_nbr_lt(0, std::numeric_limits<TypeParam>::max(),
                                    [&](vid_t nbr, const TypeParam& data) {
                                      ++lt_count;
                                      if (lt_count > 100000) {
                                        local_errors++;
                                      }
                                    });
        }
      }
      reader_errors.fetch_add(local_errors, std::memory_order_relaxed);
    });
  }

  writer.join();
  for (auto& t : readers) {
    t.join();
  }

  EXPECT_EQ(reader_errors.load(), 0)
      << "Concurrent readers detected inconsistent state";
  // Writer should have inserted all edges
  EXPECT_GE(csr.edge_num(), edge_num + kWriterIterations);
}

// Test that concurrent readers on different vertices don't interfere with
// a writer that reallocates a specific vertex's buffer.
TYPED_TEST(MutableCsrTest, TestConcurrentPutEdgeMultiVertex) {
  MutableCsr<TypeParam> csr;
  this->load_csr_data(csr, MemoryLevel::kInMemory);

  constexpr int kEdgesPerVertex = 200;
  constexpr int kReaderIterations = 1000;
  constexpr int kNumReaders = 3;

  std::atomic<bool> stop_flag{false};
  std::atomic<int> reader_errors{0};

  // Writer: insert many edges to vertex 2 (will trigger reallocation)
  std::thread writer([&]() {
    for (int i = 0; i < kEdgesPerVertex; ++i) {
      vid_t dst = static_cast<vid_t>(i % 50);
      this->template put_single_edge<MutableCsr>(csr, 2, dst, 1,
                                                 *(this->allocators[0]));
    }
    stop_flag.store(true, std::memory_order_release);
  });

  // Readers: read edges from vertex 2 (same vertex being written to)
  std::vector<std::thread> readers;
  for (int r = 0; r < kNumReaders; ++r) {
    readers.emplace_back([&]() {
      int local_errors = 0;
      for (int i = 0; i < kReaderIterations; ++i) {
        auto view = csr.get_generic_view(1);
        auto edges = view.get_edges(2);

        if (edges.end_ptr < edges.start_ptr) {
          local_errors++;
          continue;
        }

        size_t count = 0;
        for (auto it = edges.begin(); it != edges.end(); ++it) {
          ++count;
          if (count > 100000) {
            local_errors++;
            break;
          }
        }
      }
      reader_errors.fetch_add(local_errors, std::memory_order_relaxed);
    });
  }

  writer.join();
  for (auto& t : readers) {
    t.join();
  }

  EXPECT_EQ(reader_errors.load(), 0)
      << "Concurrent readers on same vertex detected inconsistent state";
}

}  // namespace test
}  // namespace neug
