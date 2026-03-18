#include <gtest/gtest.h>
#include <filesystem>

#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "unittest/utils.h"

using CsrTypes = ::testing::Types<
    neug::MutableCsr<int32_t>, neug::MutableCsr<int64_t>,
    neug::MutableCsr<float>, neug::MutableCsr<neug::EmptyType>,
    neug::MutableCsr<neug::DateTime>, neug::ImmutableCsr<uint32_t>,
    neug::ImmutableCsr<uint64_t>, neug::ImmutableCsr<double>,
    neug::ImmutableCsr<neug::Date>, neug::SingleImmutableCsr<int32_t>,
    neug::SingleImmutableCsr<int64_t>, neug::SingleImmutableCsr<float>,
    neug::SingleImmutableCsr<neug::EmptyType>,
    neug::SingleImmutableCsr<neug::DateTime>, neug::SingleMutableCsr<uint32_t>,
    neug::SingleMutableCsr<uint64_t>, neug::SingleMutableCsr<double>,
    neug::SingleMutableCsr<neug::Date>>;

template <typename T>
class CsrBatchTest : public ::testing::Test {
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
                                               typename T::data_t>>& expected) {
    std::vector<std::tuple<neug::vid_t, neug::vid_t, typename T::data_t>>
        actual;
    auto view = this->csr->get_generic_view(0);
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
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(std::get<0>(expected[i]), std::get<0>(actual[i]));
      EXPECT_EQ(std::get<1>(expected[i]), std::get<1>(actual[i]));
      EXPECT_EQ(std::get<2>(expected[i]), std::get<2>(actual[i]));
    }
  }

  std::unique_ptr<CsrType> csr = nullptr;

 private:
  std::filesystem::path temp_dir_;

  std::string GetTestName() const {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    return std::string(test_info->name());
  }
};

TYPED_TEST_SUITE(CsrBatchTest, CsrTypes);

TYPED_TEST(CsrBatchTest, OpenInsertScan) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  this->csr->open("csr", snapshot_dir.string(), work_dir.string());

  auto edges0 = generate_random_edges<typename TypeParam::data_t>(
      500, 1000, 10000,
      this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
          this->csr->csr_type() == neug::CsrType::kSingleMutable);
  auto edges1 = generate_random_edges<typename TypeParam::data_t>(
      500, 1000, 10000,
      this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
          this->csr->csr_type() == neug::CsrType::kSingleMutable);
  std::vector<std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
      edges2;
  for (auto& e : edges1) {
    std::get<0>(e) += 500;
  }
  size_t half_size0 = (edges0.size() + 1) / 2;
  for (size_t i = half_size0; i < edges0.size(); ++i) {
    edges2.push_back(edges0[i]);
  }
  edges0.resize(half_size0);
  size_t half_size1 = (edges1.size() + 1) / 2;
  for (size_t i = half_size1; i < edges1.size(); ++i) {
    edges2.push_back(edges1[i]);
  }
  edges1.resize(half_size1);

  this->csr->resize(500);
  EXPECT_EQ(this->csr->size(), 500);
  std::vector<neug::vid_t> src_list, dst_list;
  std::vector<typename TypeParam::data_t> edata_list;

  for (size_t i = 0; i < edges0.size(); ++i) {
    src_list.push_back(std::get<0>(edges0[i]));
    dst_list.push_back(std::get<1>(edges0[i]));
    edata_list.push_back(std::get<2>(edges0[i]));
  }
  this->csr->batch_put_edges(src_list, dst_list, edata_list, 0);

  std::sort(edges0.begin(), edges0.end());
  this->CheckEqual(edges0);

  src_list.clear();
  dst_list.clear();
  edata_list.clear();
  for (size_t i = 0; i < edges1.size(); ++i) {
    src_list.push_back(std::get<0>(edges1[i]));
    dst_list.push_back(std::get<1>(edges1[i]));
    edata_list.push_back(std::get<2>(edges1[i]));
    edges0.push_back(edges1[i]);
  }
  this->csr->resize(1000);
  EXPECT_EQ(this->csr->size(), 1000);
  this->csr->batch_put_edges(src_list, dst_list, edata_list, 0);

  std::sort(edges0.begin(), edges0.end());
  this->CheckEqual(edges0);

  src_list.clear();
  dst_list.clear();
  edata_list.clear();
  for (size_t i = 0; i < edges2.size(); ++i) {
    src_list.push_back(std::get<0>(edges2[i]));
    dst_list.push_back(std::get<1>(edges2[i]));
    edata_list.push_back(std::get<2>(edges2[i]));
    edges0.push_back(edges2[i]);
  }
  this->csr->batch_put_edges(src_list, dst_list, edata_list, 0);
  std::sort(edges0.begin(), edges0.end());
  this->CheckEqual(edges0);
}

TYPED_TEST(CsrBatchTest, OpenDumpOpenScan) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  // open -> dump -> open
  {
    this->csr->open("csr0", snapshot_dir.string(), work_dir.string());
    auto edges = generate_random_edges<typename TypeParam::data_t>(
        1000, 1000, 20000,
        this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
            this->csr->csr_type() == neug::CsrType::kSingleMutable);
    this->csr->resize(1000);
    std::vector<std::vector<neug::vid_t>> src_list(2), dst_list(2);
    std::vector<std::vector<typename TypeParam::data_t>> edata_list(2);
    std::vector<
        std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
        edges_part0;
    for (size_t i = 0; i < edges.size(); ++i) {
      size_t idx = i % 2;
      src_list[idx].push_back(std::get<0>(edges[i]));
      dst_list[idx].push_back(std::get<1>(edges[i]));
      edata_list[idx].push_back(std::get<2>(edges[i]));
      if (idx == 0) {
        edges_part0.push_back(edges[i]);
      }
    }
    this->csr->batch_put_edges(src_list[0], dst_list[0], edata_list[0], 0);
    this->csr->dump("csr0", snapshot_dir.string());

    this->csr.reset();
    this->csr = std::make_unique<TypeParam>();
    this->csr->open("csr0", snapshot_dir.string(), work_dir.string());
    std::sort(edges_part0.begin(), edges_part0.end());
    this->CheckEqual(edges_part0);

    this->csr->batch_put_edges(src_list[1], dst_list[1], edata_list[1], 0);
    std::sort(edges.begin(), edges.end());
    this->CheckEqual(edges);
    this->csr.reset();
  }
  // open_in_memory -> dump -> open
  {
    this->csr = std::make_unique<TypeParam>();
    this->csr->open_in_memory(snapshot_dir.string() + "/csr1");
    this->csr->resize(1000);
    auto edges = generate_random_edges<typename TypeParam::data_t>(
        1000, 1000, 20000,
        this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
            this->csr->csr_type() == neug::CsrType::kSingleMutable);
    this->csr->resize(1000);
    std::vector<std::vector<neug::vid_t>> src_list(2), dst_list(2);
    std::vector<std::vector<typename TypeParam::data_t>> edata_list(2);
    std::vector<
        std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
        edges_part0;
    for (size_t i = 0; i < edges.size(); ++i) {
      size_t idx = i % 2;
      src_list[idx].push_back(std::get<0>(edges[i]));
      dst_list[idx].push_back(std::get<1>(edges[i]));
      edata_list[idx].push_back(std::get<2>(edges[i]));
      if (idx == 0) {
        edges_part0.push_back(edges[i]);
      }
    }
    this->csr->batch_put_edges(src_list[0], dst_list[0], edata_list[0], 0);
    this->csr->dump("csr1", snapshot_dir.string());

    this->csr.reset();
    this->csr = std::make_unique<TypeParam>();
    this->csr->open("csr1", snapshot_dir.string(), work_dir.string());
    std::sort(edges_part0.begin(), edges_part0.end());
    this->CheckEqual(edges_part0);

    this->csr->batch_put_edges(src_list[1], dst_list[1], edata_list[1], 0);
    std::sort(edges.begin(), edges.end());
    this->CheckEqual(edges);
    this->csr.reset();
  }
  // open_in_memory -> dump -> open_in_memory
  {
    this->csr = std::make_unique<TypeParam>();
    this->csr->open_in_memory(snapshot_dir.string() + "/csr2");
    auto edges = generate_random_edges<typename TypeParam::data_t>(
        1000, 1000, 20000,
        this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
            this->csr->csr_type() == neug::CsrType::kSingleMutable);
    this->csr->resize(1000);
    std::vector<std::vector<neug::vid_t>> src_list(2), dst_list(2);
    std::vector<std::vector<typename TypeParam::data_t>> edata_list(2);
    std::vector<
        std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
        edges_part0;
    for (size_t i = 0; i < edges.size(); ++i) {
      size_t idx = i % 2;
      src_list[idx].push_back(std::get<0>(edges[i]));
      dst_list[idx].push_back(std::get<1>(edges[i]));
      edata_list[idx].push_back(std::get<2>(edges[i]));
      if (idx == 0) {
        edges_part0.push_back(edges[i]);
      }
    }
    this->csr->batch_put_edges(src_list[0], dst_list[0], edata_list[0], 0);
    this->csr->dump("csr2", snapshot_dir.string());

    this->csr.reset();
    this->csr = std::make_unique<TypeParam>();
    this->csr->open_in_memory(snapshot_dir.string() + "/csr2");
    this->csr->resize(1000);
    std::sort(edges_part0.begin(), edges_part0.end());
    this->CheckEqual(edges_part0);

    this->csr->batch_put_edges(src_list[1], dst_list[1], edata_list[1], 0);
    std::sort(edges.begin(), edges.end());
    this->CheckEqual(edges);
    this->csr.reset();
  }
  // open -> dump -> open_in_memory
  {
    this->csr = std::make_unique<TypeParam>();
    this->csr->open("csr3", snapshot_dir.string(), work_dir.string());
    auto edges = generate_random_edges<typename TypeParam::data_t>(
        1000, 1000, 20000,
        this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
            this->csr->csr_type() == neug::CsrType::kSingleMutable);
    this->csr->resize(1000);
    std::vector<std::vector<neug::vid_t>> src_list(2), dst_list(2);
    std::vector<std::vector<typename TypeParam::data_t>> edata_list(2);
    std::vector<
        std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
        edges_part0;
    for (size_t i = 0; i < edges.size(); ++i) {
      size_t idx = i % 2;
      src_list[idx].push_back(std::get<0>(edges[i]));
      dst_list[idx].push_back(std::get<1>(edges[i]));
      edata_list[idx].push_back(std::get<2>(edges[i]));
      if (idx == 0) {
        edges_part0.push_back(edges[i]);
      }
    }
    this->csr->batch_put_edges(src_list[0], dst_list[0], edata_list[0], 0);
    this->csr->dump("csr3", snapshot_dir.string());

    this->csr.reset();
    this->csr = std::make_unique<TypeParam>();
    this->csr->open_in_memory(snapshot_dir.string() + "/csr3");
    this->csr->resize(1000);
    std::sort(edges_part0.begin(), edges_part0.end());
    this->CheckEqual(edges_part0);

    this->csr->batch_put_edges(src_list[1], dst_list[1], edata_list[1], 0);
    std::sort(edges.begin(), edges.end());
    this->CheckEqual(edges);
    this->csr.reset();
  }
}

TYPED_TEST(CsrBatchTest, DeleteVertices) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  this->csr->open("csr", snapshot_dir.string(), work_dir.string());

  auto edges = generate_random_edges<typename TypeParam::data_t>(
      1000, 1000, 10000,
      this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
          this->csr->csr_type() == neug::CsrType::kSingleMutable);
  this->csr->resize(1000);
  EXPECT_EQ(this->csr->size(), 1000);
  std::vector<neug::vid_t> src_list, dst_list;
  std::vector<typename TypeParam::data_t> edata_list;

  std::vector<neug::vid_t> src_del =
      generate_random_vertices<neug::vid_t>(1000, 100);
  std::vector<neug::vid_t> dst_del =
      generate_random_vertices<neug::vid_t>(1000, 100);

  std::set<neug::vid_t> src_del_set(src_del.begin(), src_del.end());
  std::set<neug::vid_t> dst_del_set(dst_del.begin(), dst_del.end());

  std::vector<std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
      edges_after_delete;
  for (size_t i = 0; i < edges.size(); ++i) {
    if (src_del_set.count(std::get<0>(edges[i])) == 0 &&
        dst_del_set.count(std::get<1>(edges[i])) == 0) {
      edges_after_delete.push_back(edges[i]);
    }
    src_list.push_back(std::get<0>(edges[i]));
    dst_list.push_back(std::get<1>(edges[i]));
    edata_list.push_back(std::get<2>(edges[i]));
  }
  this->csr->batch_put_edges(src_list, dst_list, edata_list, 0);
  this->csr->batch_delete_vertices(src_del_set, dst_del_set);

  std::sort(edges_after_delete.begin(), edges_after_delete.end());
  this->CheckEqual(edges_after_delete);
}

TYPED_TEST(CsrBatchTest, DeleteEdges) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  this->csr->open("csr", snapshot_dir.string(), work_dir.string());

  auto edges = generate_random_edges<typename TypeParam::data_t>(
      1000, 1000, 10000,
      this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
          this->csr->csr_type() == neug::CsrType::kSingleMutable);
  this->csr->resize(1000);
  EXPECT_EQ(this->csr->size(), 1000);
  std::vector<neug::vid_t> src_list, dst_list;
  std::vector<typename TypeParam::data_t> edata_list;
  std::set<std::pair<neug::vid_t, neug::vid_t>> delete_edge_set;

  for (size_t i = 0; i < edges.size() * 0.2; ++i) {
    delete_edge_set.insert({std::get<0>(edges[i]), std::get<1>(edges[i])});
  }

  std::vector<std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
      edges_after_delete;
  for (size_t i = 0; i < edges.size(); ++i) {
    if (delete_edge_set.count({std::get<0>(edges[i]), std::get<1>(edges[i])}) ==
        0) {
      edges_after_delete.push_back(edges[i]);
    }
    src_list.push_back(std::get<0>(edges[i]));
    dst_list.push_back(std::get<1>(edges[i]));
    edata_list.push_back(std::get<2>(edges[i]));
  }
  this->csr->batch_put_edges(src_list, dst_list, edata_list, 0);
  std::vector<std::pair<neug::vid_t, int32_t>> edges_to_delete;
  auto view = this->csr->get_generic_view(0);
  for (const auto& edge : delete_edge_set) {
    neug::vid_t src = edge.first;
    neug::vid_t dst = edge.second;
    auto es = view.get_edges(src);
    for (auto it = es.begin(); it != es.end(); ++it) {
      if (it.get_vertex() == dst) {
        edges_to_delete.push_back(
            {src, (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                   reinterpret_cast<const char*>(es.start_ptr)) /
                      it.cfg.stride});
      }
    }
  }
  this->csr->batch_delete_edges(edges_to_delete);

  std::sort(edges_after_delete.begin(), edges_after_delete.end());
  this->CheckEqual(edges_after_delete);
}

TYPED_TEST(CsrBatchTest, DeleteEdgesAndCompact) {
  auto work_dir = this->WorkDirectory();
  auto snapshot_dir = this->SnapshotDirectory();
  this->csr->open("csr", snapshot_dir.string(), work_dir.string());

  auto edges = generate_random_edges<typename TypeParam::data_t>(
      1000, 1000, 10000,
      this->csr->csr_type() == neug::CsrType::kSingleImmutable ||
          this->csr->csr_type() == neug::CsrType::kSingleMutable);
  this->csr->resize(1000);
  EXPECT_EQ(this->csr->size(), 1000);
  std::vector<neug::vid_t> src_list, dst_list;
  std::vector<typename TypeParam::data_t> edata_list;
  std::set<std::pair<neug::vid_t, neug::vid_t>> delete_edge_set;

  for (size_t i = 0; i < edges.size() * 0.2; ++i) {
    delete_edge_set.insert({std::get<0>(edges[i]), std::get<1>(edges[i])});
  }

  std::vector<std::tuple<neug::vid_t, neug::vid_t, typename TypeParam::data_t>>
      edges_after_delete;
  for (size_t i = 0; i < edges.size(); ++i) {
    if (delete_edge_set.count({std::get<0>(edges[i]), std::get<1>(edges[i])}) ==
        0) {
      edges_after_delete.push_back(edges[i]);
    }
    src_list.push_back(std::get<0>(edges[i]));
    dst_list.push_back(std::get<1>(edges[i]));
    edata_list.push_back(std::get<2>(edges[i]));
  }
  this->csr->batch_put_edges(src_list, dst_list, edata_list, 0);
  auto view = this->csr->get_generic_view(0);
  for (const auto& edge : delete_edge_set) {
    neug::vid_t src = edge.first;
    neug::vid_t dst = edge.second;
    auto es = view.get_edges(src);
    for (auto it = es.begin(); it != es.end(); ++it) {
      if (it.get_vertex() == dst) {
        int32_t offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                          reinterpret_cast<const char*>(es.start_ptr)) /
                         it.cfg.stride;
        this->csr->delete_edge(src, offset, 0);
      }
    }
  }
  std::sort(edges_after_delete.begin(), edges_after_delete.end());
  this->CheckEqual(edges_after_delete);
  this->csr->compact();
  this->CheckEqual(edges_after_delete);
}