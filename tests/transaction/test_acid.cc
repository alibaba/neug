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
#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include "neug/config.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"

#define SLEEP_TIME_MILLI_SEC 1

namespace fs = std::filesystem;
using namespace neug;
using neug::EdgeStrategy;
using neug::MemoryLevel;
using neug::NeugDB;
using neug::NeugDBSession;
using oid_t = int64_t;
using neug::DataTypeId;
using neug::Schema;
using neug::vid_t;

// Utility: Generate unique id (thread-safe)
static std::atomic<int64_t> neug_current_id(0);
Property neug_generate_id() {
  return neug::Property::From(neug_current_id.fetch_add(1));
}

std::string neug_generate_random_string(int length) {
  static const char alphanum[] = "abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);
  for (int i = 0; i < length; ++i) {
    ret += alphanum[dist(gen)];
  }
  return ret;
}

std::string neug_generate_work_dir(const std::string& prefix) {
  while (true) {
    std::string dir = prefix + neug_generate_random_string(8);
    if (fs::exists(dir))
      continue;
    fs::create_directories(dir);
    return dir;
  }
}

// Test fixture for neug ACID tests
class NeugDBACIDTest : public ::testing::Test {
 protected:
  void SetUp() override {
    work_dir_ = neug_generate_work_dir("/tmp/neug_acid/");
    thread_num_ = 16;
  }
  void TearDown() override { fs::remove_all(work_dir_); }
  std::string work_dir_;
  int32_t thread_num_;
};

// Parallel helpers
template <typename FUNC_T>
void neug_parallel_transaction(std::shared_ptr<neug::NeugDBService> svc,
                               const FUNC_T& func, int txn_num) {
  std::vector<int> txn_ids(txn_num);
  std::iota(txn_ids.begin(), txn_ids.end(), 0);
  std::shuffle(txn_ids.begin(), txn_ids.end(),
               std::mt19937(std::random_device()()));
  int thread_num = svc->SessionNum();
  std::vector<std::thread> threads;
  std::atomic<int> txn_counter(0);
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          auto guard = svc->AcquireSession();
          neug::NeugDBSession& session = *guard.get();
          while (true) {
            int txn_id = txn_counter.fetch_add(1);
            if (txn_id >= txn_num)
              break;
            func(session, txn_ids[txn_id]);
          }
        },
        i);
  }
  for (auto& t : threads)
    t.join();
}

template <typename FUNC_T>
void neug_parallel_client(std::shared_ptr<neug::NeugDBService> svc,
                          const FUNC_T& func) {
  int thread_num = svc->SessionNum();
  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          auto guard = svc->AcquireSession();
          neug::NeugDBSession& session = *guard.get();
          func(session, tid);
        },
        i);
  }
  for (auto& t : threads)
    t.join();
}

// Helper: get random vertex iterator
bool neug_get_random_vertex(const StorageReadInterface& txn, label_t label_id,
                            vid_t& vid) {
  auto vertex_set = txn.GetVertexSet(label_id);
  int vnum = 0;
  for ([[maybe_unused]] auto v : vertex_set) {
    ++vnum;
  }
  if (vnum == 0)
    return false;
  std::random_device rand_dev;
  std::mt19937 gen(rand_dev());
  std::uniform_int_distribution<vid_t> dist(0, vnum - 1);
  int picked = dist(gen);
  auto v1 = txn.GetVertexSet(label_id);
  for (auto v : vertex_set) {
    if (picked == 0) {
      vid = v;
      return true;
    }
    --picked;
  }
  return false;
}

auto neug_get_random_vertex(StorageTPUpdateInterface& gi, label_t label_id) {
  auto vertex_set = gi.GetVertexSet(label_id);
  int num = 0;
  neug::vid_t vid = 0;
  for ([[maybe_unused]] auto v : vertex_set) {
    ++num;
  }

  if (num == 0)
    return vid;
  std::random_device rand_dev;
  std::mt19937 gen(rand_dev());
  std::uniform_int_distribution<int> dist(0, num - 1);
  int picked = dist(gen);
  auto v1 = gi.GetVertexSet(label_id);
  for (auto v : vertex_set) {
    if (picked == 0) {
      return v;
    }
    --picked;
  }
  return vid;
}

// Helper: append string to field
void neug_append_string_to_field(StorageTPUpdateInterface& gui, label_t label,
                                 neug::vid_t vit, int col_id,
                                 const std::string& str) {
  std::string cur_str =
      std::string(gui.GetVertexProperty(label, vit, col_id).as_string_view());
  if (cur_str.empty())
    cur_str = str;
  else {
    cur_str += ";";
    cur_str += str;
  }
  gui.UpdateVertexProperty(label, vit, col_id, Property::From(cur_str));
}

// Atomicity helpers and tests
std::shared_ptr<neug::NeugDBService> neug_AtomicityInit(
    NeugDB& db, const std::string& work_dir, int thread_num) {
  db.Open(work_dir, thread_num);
  auto service = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id2 INT64, name STRING, "
        "emails STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE KNOWS(FROM PERSON TO PERSON, since INT64);"));
  }

  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = service->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t id1 = 1;
  std::string name1 = "Alice";
  std::string email1 = "alice@aol.com";
  int64_t id2 = 2;
  std::string name2 = "Bob";
  std::string email2 = "bob@hotmail.com;bobby@yahoo.com";
  vid_t vid;
  EXPECT_TRUE(gii.AddVertex(
      person_label_id, neug_generate_id(),
      {Property::from_int64(id1), Property::from_string_view(name1),
       Property::from_string_view(email1)},
      vid));
  EXPECT_TRUE(gii.AddVertex(
      person_label_id, neug_generate_id(),
      {Property::from_int64(id2), Property::from_string_view(name2),
       Property::from_string_view(email2)},
      vid));
  txn.Commit();

  return service;
}

bool neug_AtomicityC(neug::NeugDBSession& db, int64_t person2_id,
                     const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");
  auto vit = neug_get_random_vertex(gui, person_label_id);
  neug_append_string_to_field(gui, person_label_id, vit, 2, new_email);
  auto p2_id = neug_generate_id();
  std::string name = "", email = "";
  vid_t vid;

  if (!gui.AddVertex(
          person_label_id, p2_id,
          {Property::from_int64(person2_id), Property::from_string_view(name),
           Property::from_string_view(email)},
          vid)) {
    txn.Abort();
    return false;
  }
  if (!gui.AddEdge(person_label_id, vit, person_label_id, vid, knows_label_id,
                   {Property::from_int64(since)})) {
    txn.Abort();
    return false;
  }
  txn.Commit();
  return true;
}

bool neug_AtomicityRB(neug::NeugDBSession& db, int64_t person2_id,
                      const std::string& new_email, int64_t since) {
  UpdateTransaction txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit1 = neug_get_random_vertex(gui, person_label_id);
  neug_append_string_to_field(gui, person_label_id, vit1, 2, new_email);
  neug::vid_t vit2;
  if (gui.GetVertexIndex(person_label_id,
                         neug::Property::from_int64(person2_id), vit2)) {
    txn.Abort();
    return false;
  }
  auto p2_id = neug_generate_id();
  std::string name = "", email = "";
  vid_t vid;
  EXPECT_TRUE(gui.AddVertex(
      person_label_id, p2_id,
      {Property::from_int64(person2_id), Property::from_string_view(name),
       Property::from_string_view(email)},
      vid));
  auto ret = txn.Commit();
  EXPECT_TRUE(ret);
  return true;
}

int64_t neug_count_email_num(const std::string_view& sv) {
  if (sv.empty())
    return 0;
  int64_t ret = 1;
  for (auto c : sv)
    if (c == ';')
      ++ret;
  return ret;
}

std::pair<int64_t, int64_t> neug_AtomicityCheck(
    std::shared_ptr<neug::NeugDBService> svc) {
  auto sess = svc->AcquireSession();
  auto txn = sess->GetReadTransaction();
  const auto& db = svc->db();
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  int64_t num_persons = 0, num_emails = 0;
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vprop_accessor = std::dynamic_pointer_cast<
      StorageReadInterface::vertex_column_t<std::string_view>>(
      gi.GetVertexPropColumn(person_label_id, "emails"));
  auto vset = gi.GetVertexSet(person_label_id);
  for (vid_t lid : vset) {
    ++num_persons;
    num_emails += neug_count_email_num(vprop_accessor->get_view(lid));
  }
  return {num_persons, num_emails};
}

// Dirty Writes

std::shared_ptr<neug::NeugDBService> G0Init(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE PERSON (id1 INT64, id2 INT64, "
                    "versionHistory STRING, "
                    "PRIMARY KEY(id1));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE KNOWS(FROM PERSON TO PERSON, versionHistory "
        "STRING);"));
  }

  const auto& schema = db.schema();
  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto knows_label_id = schema.get_edge_label_id("KNOWS");

  db.graph().EnsureCapacity(person_label_id, 1000);
  db.graph().EnsureCapacity(person_label_id, person_label_id, knows_label_id,
                            1000);

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);

  std::string value = "0";
  for (int i = 0; i < 100; ++i) {
    auto p1_id = neug_generate_id();
    int64_t p1_id_property = 2 * i + 1;
    vid_t vid0, vid1;
    CHECK(gii.AddVertex(person_label_id, p1_id,
                        {Property::from_int64(p1_id_property),
                         Property::from_string_view(value)},
                        vid0));
    auto p2_id = neug_generate_id();
    int64_t p2_id_property = 2 * i + 2;
    CHECK(gii.AddVertex(person_label_id, p2_id,
                        {Property::from_int64(p2_id_property),
                         Property::from_string_view(value)},
                        vid1));
    CHECK(gii.AddEdge(person_label_id, vid0, person_label_id, vid1,
                      knows_label_id, {Property::from_string_view(value)}));
  }
  txn.Commit();
  return svc;
}

void G0(neug::NeugDBSession& db, int64_t person1_id, int64_t person2_id,
        int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  neug::vid_t vit1;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  bool flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person1_id) {
      vit1 = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  neug_append_string_to_field(gui, person_label_id, vit1, 1,
                              std::to_string(txn_id));

  neug::vid_t vit2;
  flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person2_id) {
      vit2 = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  neug_append_string_to_field(gui, person_label_id, vit2, 1,
                              std::to_string(txn_id));

  auto oe_view = gui.GetGenericOutgoingGraphView(
      person_label_id, person_label_id, knows_label_id);
  auto oe_edge = oe_view.get_edges(vit1);
  auto oeit = oe_edge.begin();
  auto oeit_end = oe_edge.end();
  for (; oeit != oeit_end; ++oeit) {
    if (oeit.get_vertex() == vit2) {
      break;
    }
  }
  auto ed_accessor = gui.GetEdgeDataAccessor(person_label_id, knows_label_id,
                                             person_label_id, 0);
  CHECK(oeit != oeit_end);

  Property cur = ed_accessor.get_data(oeit);
  std::string cur_str(cur.as_string_view());
  if (cur_str.empty()) {
    cur_str = std::to_string(txn_id);
  } else {
    cur_str += ";";
    cur_str += std::to_string(txn_id);
  }
  Property new_value;
  new_value.set_string_view(cur_str);

  ed_accessor.set_data(oeit, new_value, txn.timestamp());

  txn.Commit();
}

std::tuple<std::string, std::string, std::string> G0Check(
    NeugDB& db, std::shared_ptr<neug::NeugDBService> svc, int64_t person1_id,
    int64_t person2_id) {
  auto sess = svc->AcquireSession();
  auto txn = sess->GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  auto prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id2"));

  auto name_col = std::dynamic_pointer_cast<
      StorageReadInterface::vertex_column_t<std::string_view>>(
      gi.GetVertexPropColumn(person_label_id, "versionHistory"));

  std::string p1_version_history;
  vid_t vit1_index = 0;
  auto vertex_set = gi.GetVertexSet(person_label_id);
  for (vid_t lid : vertex_set) {
    if (prop_col->get(lid).as_int64() == person1_id) {
      vit1_index = lid;
      p1_version_history = std::string(name_col->get(lid).as_string_view());
      break;
    }
  }

  vid_t vit2_index = 0;
  std::string p2_version_history;

  for (vid_t lid : vertex_set) {
    if (prop_col->get(lid).as_int64() == person2_id) {
      vit2_index = lid;
      p2_version_history = std::string(name_col->get(lid).as_string_view());
      break;
    }
  }

  auto view = gi.GetGenericOutgoingGraphView(person_label_id, person_label_id,
                                             knows_label_id);
  auto oeit = view.get_edges(vit1_index);
  NbrIterator iter = oeit.begin();
  auto end = oeit.end();
  for (; iter != end; ++iter) {
    if ((*iter) == vit2_index) {
      break;
    }
  }
  auto ed_accessor = gi.GetEdgeDataAccessor(person_label_id, knows_label_id,
                                            person_label_id, 0);

  CHECK(iter != end);
  Property k_version_history_field = ed_accessor.get_data(iter);
  CHECK(k_version_history_field.type() == neug::DataTypeId::kVarchar);
  std::string k_version_history(k_version_history_field.as_string_view());

  return std::make_tuple(p1_version_history, p2_version_history,
                         k_version_history);
}

// Intermediate Reads

std::shared_ptr<neug::NeugDBService> G1BInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t value = 99;
  for (int i = 0; i < 100; ++i) {
    int64_t vertex_id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(vertex_id_property), Property::from_int64(value)},
        vid));
  }
  txn.Commit();
  return svc;
}

void G1B1(neug::NeugDBSession& db, int64_t even, int64_t odd) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit = neug_get_random_vertex(gui, person_label_id);
  gui.UpdateVertexProperty(person_label_id, vit, 1, Property::From(even));
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  gui.UpdateVertexProperty(person_label_id, vit, 1, Property::From(odd));
  txn.Commit();
}

int64_t G1B2(neug::NeugDBSession& db) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());

  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  vid_t vid;
  CHECK(neug_get_random_vertex(gi, person_label_id, vid));
  auto vprop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));
  CHECK(vprop_col != nullptr);
  return vprop_col->get(vid).as_int64();
}

// Circular Information Flow

std::shared_ptr<neug::NeugDBService> G1CInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();

  StorageTPInsertInterface gii(txn);

  int64_t version_property = 0;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(id_property),
                         Property::from_int64(version_property)},
                        vid));
  }
  txn.Commit();
  return svc;
}

int64_t G1C(neug::NeugDBSession& db, int64_t person1_id, int64_t person2_id,
            int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  neug::vid_t person1_vid;
  bool flag = false;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person2_id) {
      person1_vid = v;
      flag = true;
      break;
    }
  }
  gui.UpdateVertexProperty(person_label_id, person1_vid, 1,
                           Property::From(txn_id));

  CHECK(flag);
  neug::vid_t person2_vid;
  flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person1_id) {
      person2_vid = v;
      flag = true;
      break;
    }
  }
  int64_t ret =
      gui.GetVertexProperty(person_label_id, person2_vid, 1).as_int64();

  txn.Commit();

  return ret;
}

// Aborted Reads

std::shared_ptr<neug::NeugDBService> G1AInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t vertex_data = 1;
  for (int i = 0; i < 100; ++i) {
    int64_t vertex_id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(vertex_id_property),
                         Property::from_int64(vertex_data)},
                        vid));
  }
  txn.Commit();
  return svc;
}

void G1A1(neug::NeugDBSession& db) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  // select a random person
  auto vit = neug_get_random_vertex(gui, person_label_id);

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  // attempt to set version = 2
  gui.UpdateVertexProperty(person_label_id, vit, 1, Property::From<int64_t>(2));
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  txn.Abort();
}

int64_t G1A2(neug::NeugDBSession& db) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());

  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  vid_t vid;
  CHECK(neug_get_random_vertex(gi, person_label_id, vid));
  auto vprop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));
  CHECK(vprop_col != nullptr);
  return vprop_col->get(vid).as_int64();
}

// Item-Many-Preceders

std::shared_ptr<neug::NeugDBService> IMPInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  int64_t version_property = 1;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    vid_t vid;
    CHECK(txn.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(id_property),
                         Property::from_int64(version_property)},
                        vid));
  }
  txn.Commit();
  return svc;
}

void IMP1(neug::NeugDBSession& db) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  StorageTPUpdateInterface gui(txn);
  auto vit = neug_get_random_vertex(gui, person_label_id);
  int64_t old_version =
      gui.GetVertexProperty(person_label_id, vit, 1).as_int64();
  gui.UpdateVertexProperty(person_label_id, vit, 1,
                           Property::From(old_version + 1));
  txn.Commit();
}

std::tuple<int64_t, int64_t> IMP2(neug::NeugDBSession& db, int64_t person1_id) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  vid_t vit0_index = 0;
  auto v_prop_col0 =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto v_prop_col1 =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));
  CHECK(v_prop_col0 != nullptr);
  auto vertex_set = gi.GetVertexSet(person_label_id);
  bool found = false;
  for (vid_t lid : vertex_set) {
    if (v_prop_col0->get(lid).as_int64() == person1_id) {
      vit0_index = lid;
      found = true;
      break;
    }
  }
  CHECK(found);

  int64_t v1 = v_prop_col1->get(vit0_index).as_int64();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  vid_t vit1_index = 0;
  for (vid_t lid : vertex_set) {
    if (v_prop_col0->get(lid).as_int64() == person1_id) {
      vit1_index = lid;
      break;
    }
  }

  int64_t v2 = v_prop_col1->get(vit1_index).as_int64();

  return std::make_tuple(v1, v2);
}

// Predicate-Many-Preceders

std::shared_ptr<neug::NeugDBService> PMPInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE PERSON (id INT64, id_prop INT64, "
                    "PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE POST (id INT64, id_prop INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query("CREATE REL TABLE LIKES(FROM PERSON TO POST);"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto post_label_id = schema.get_vertex_label_id("POST");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  for (int i = 0; i < 100; ++i) {
    int64_t value = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(value)}, vid));
    CHECK(gii.AddVertex(post_label_id, neug_generate_id(),
                        {Property::from_int64(value)}, vid));
  }
  txn.Commit();
  return svc;
}

bool PMP1(neug::NeugDBSession& db, int64_t person_id, int64_t post_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto post_label_id = db.schema().get_vertex_label_id("POST");
  auto likes_label_id = db.schema().get_edge_label_id("LIKES");
  neug::vid_t person_vid;
  bool found = false;
  auto vertex_set = gui.GetVertexSet(person_label_id);
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person_id) {
      person_vid = v;
      found = true;
      break;
    }
  }
  CHECK(found);
  neug::vid_t post_vid;
  found = false;
  auto post_vertex_set = gui.GetVertexSet(post_label_id);
  for (auto v : post_vertex_set) {
    int64_t v_id = gui.GetVertexProperty(post_label_id, v, 0).as_int64();
    if (v_id == post_id) {
      post_vid = v;
      found = true;
      break;
    }
  }
  CHECK(found);
  if (!txn.AddEdge(person_label_id, person_vid, post_label_id, post_vid,
                   likes_label_id, {})) {
    txn.Abort();
    return false;
  }
  txn.Commit();
  return true;
}

std::tuple<int64_t, int64_t> PMP2(neug::NeugDBSession& db, int64_t post_id) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto post_label_id = db.schema().get_vertex_label_id("POST");
  auto likes_label_id = db.schema().get_edge_label_id("LIKES");

  vid_t vit0_index = 0;
  auto v_prop_col0 =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(post_label_id, "id_prop"));
  CHECK(v_prop_col0 != nullptr);
  auto vertex_set = gi.GetVertexSet(post_label_id);
  for (vid_t lid : vertex_set) {
    if (v_prop_col0->get(lid).as_int64() == post_id) {
      vit0_index = lid;
      break;
    }
  }
  int64_t c1 = 0;
  auto view = gi.GetGenericIncomingGraphView(post_label_id, likes_label_id,
                                             person_label_id);
  auto ieit = view.get_edges(vit0_index);
  for (auto iter = ieit.begin(); iter != ieit.end(); ++iter) {
    c1++;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  auto v_prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(post_label_id, "id_prop"));
  CHECK(v_prop_col != nullptr);
  vid_t vit1_index = 0;
  auto vertex_set1 = gi.GetVertexSet(post_label_id);
  for (vid_t lid : vertex_set1) {
    if (v_prop_col->get(lid).as_int64() == post_id) {
      vit1_index = lid;
      break;
    }
  }
  int64_t c2 = 0;
  auto view2 = gi.GetGenericIncomingGraphView(post_label_id, likes_label_id,
                                              person_label_id);
  auto ieit2 = view2.get_edges(vit1_index);
  for (auto iter = ieit2.begin(); iter != ieit2.end(); ++iter) {
    c2++;
  }
  return std::make_tuple(c1, c2);
}

// Observed Transaction Vanishes

std::shared_ptr<neug::NeugDBService> OTVInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, name STRING, "
        "version INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query("CREATE REL TABLE KNOWS(FROM PERSON TO PERSON);"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto knows_label_id = schema.get_edge_label_id("KNOWS");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t value = 0;
  std::vector<std::string> string_props;
  for (int j = 1; j <= 100; j++) {
    std::vector<vid_t> vids;
    for (int i = 1; i <= 4; i++) {
      auto id = neug_generate_id();
      int64_t id_property = j * 4 + i;
      vid_t vid;
      string_props.push_back(std::to_string(j));
      CHECK(gii.AddVertex(person_label_id, id,
                          {Property::from_int64(id_property),
                           Property::from_string_view(string_props.back()),
                           Property::from_int64(value)},
                          vid));
      vids.push_back(vid);
    }
    for (int i = 0; i < 4; i++) {
      CHECK(gii.AddEdge(person_label_id, vids[i], person_label_id,
                        vids[(i + 1) % 4], knows_label_id, {}));
    }
  }
  txn.Commit();
  return svc;
}

void OTV1(neug::NeugDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");
  vid_t vid1;

  bool found = false;
  auto vertex_set = gui.GetVertexSet(person_label_id);
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person_id) {
      vid1 = v;
      found = true;
      break;
    }
  }
  CHECK(found);
  auto oe_view = gui.GetGenericOutgoingGraphView(
      person_label_id, person_label_id, knows_label_id);
  auto vid1_edges = oe_view.get_edges(vid1);

  for (auto eit1 = vid1_edges.begin(); eit1 != vid1_edges.end(); ++eit1) {
    vid_t vid2 = eit1.get_vertex();
    auto vid2_edges = oe_view.get_edges(vid2);
    for (auto eit2 = vid2_edges.begin(); eit2 != vid2_edges.end(); ++eit2) {
      vid_t vid3 = eit2.get_vertex();
      auto vid3_edges = oe_view.get_edges(vid3);
      for (auto eit3 = vid3_edges.begin(); eit3 != vid3_edges.end(); ++eit3) {
        vid_t vid4 = eit3.get_vertex();
        auto vid4_edges = oe_view.get_edges(vid4);
        for (auto eit4 = vid4_edges.begin(); eit4 != vid4_edges.end(); ++eit4) {
          if (eit4.get_vertex() == vid1) {
            txn.UpdateVertexProperty(
                person_label_id, vid1, 2,
                Property::From(
                    txn.GetVertexProperty(person_label_id, vid1, 2).as_int64() +
                    1));

            gui.UpdateVertexProperty(
                person_label_id, vid2, 2,
                Property::From(
                    gui.GetVertexProperty(person_label_id, vid2, 2).as_int64() +
                    1));

            gui.UpdateVertexProperty(
                person_label_id, vid3, 2,
                Property::From(
                    gui.GetVertexProperty(person_label_id, vid3, 2).as_int64() +
                    1));

            gui.UpdateVertexProperty(
                person_label_id, vid4, 2,
                Property::From(
                    gui.GetVertexProperty(person_label_id, vid4, 2).as_int64() +
                    1));

            txn.Commit();
            return;
          }
        }
      }
    }
  }
}

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
OTV2(neug::NeugDBSession& db, int64_t person_id) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  auto view = gi.GetGenericOutgoingGraphView(person_label_id, person_label_id,
                                             knows_label_id);
  auto prop0_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto vprop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));

  auto get_versions = [&]() -> std::tuple<int64_t, int64_t, int64_t, int64_t> {
    auto vertex_set = gi.GetVertexSet(person_label_id);
    for (vid_t lid : vertex_set) {
      auto edges1 = view.get_edges(lid);
      for (auto it = edges1.begin(); it != edges1.end(); ++it) {
        vid_t vid2 = it.get_vertex();
        auto edges2 = view.get_edges(vid2);
        for (auto it2 = edges2.begin(); it2 != edges2.end(); ++it2) {
          vid_t vid3 = it2.get_vertex();
          auto edges3 = view.get_edges(vid3);
          for (auto it3 = edges3.begin(); it3 != edges3.end(); ++it3) {
            vid_t vid4 = it3.get_vertex();
            auto edges4 = view.get_edges(vid4);
            for (auto it4 = edges4.begin(); it4 != edges4.end(); ++it4) {
              if (it4.get_vertex() == lid) {
                int64_t v1_version = vprop_col->get(lid).as_int64();
                int64_t v2_version = vprop_col->get(vid2).as_int64();
                int64_t v3_version = vprop_col->get(vid3).as_int64();
                int64_t v4_version = vprop_col->get(vid4).as_int64();
                return std::make_tuple(v1_version, v2_version, v3_version,
                                       v4_version);
              }
            }
          }
        }
      }
    }
    return std::make_tuple(0, 0, 0, 0);
  };
  {
    auto vertex_set = gi.GetVertexSet(person_label_id);
    bool found = false;

    for (vid_t lid : vertex_set) {
      if (prop0_col->get(lid).as_int64() == person_id) {
        found = true;
        break;
      }
    }

    CHECK(found);
  }
  auto tup1 = get_versions();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  {
    auto vertex_set = gi.GetVertexSet(person_label_id);
    bool found = false;
    for (vid_t lid : vertex_set) {
      if (prop0_col->get(lid).as_int64() == person_id) {
        found = true;
        break;
      }
    }
    CHECK(found);
  }
  auto tup2 = get_versions();

  return std::make_tuple(tup1, tup2);
}

// Fractured Reads

std::shared_ptr<neug::NeugDBService> FRInit(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  return OTVInit(db, work_dir, thread_num);
}

void FR1(neug::NeugDBSession& db, int64_t person_id) { OTV1(db, person_id); }

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
FR2(neug::NeugDBSession& db, int64_t person_id) {
  return OTV2(db, person_id);
}

// Lost Updates

std::shared_ptr<neug::NeugDBService> LUInit(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE PERSON (id INT64, id_prop INT64, "
                    "num_friends INT64, "
                    "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();
  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t num_property = 0;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(id_property), Property::from_int64(num_property)},
        vid));
  }

  txn.Commit();
  return svc;
}

bool LU1(neug::NeugDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  neug::vid_t person_vid;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  bool flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person_id) {
      person_vid = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);

  int64_t num_friends =
      gui.GetVertexProperty(person_label_id, person_vid, 1).as_int64();
  gui.UpdateVertexProperty(person_label_id, person_vid, 1,
                           Property::From(num_friends + 1));

  txn.Commit();
  return true;
}

std::map<int64_t, int64_t> LU2(neug::NeugDBSession& db) {
  std::map<int64_t, int64_t> numFriends;
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto num_friends_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "num_friends"));
  auto vertex_set = gi.GetVertexSet(person_label_id);
  for (vid_t lid : vertex_set) {
    int64_t person_id = prop_col->get(lid).as_int64();
    int64_t num_friends = num_friends_col->get(lid).as_int64();
    numFriends.emplace(person_id, num_friends);
  }

  return numFriends;
}

// Write Skews

std::shared_ptr<neug::NeugDBService> WSInit(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();
  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gi(txn);

  for (int i = 1; i <= 100; i++) {
    int64_t id1 = 2 * i - 1;
    int64_t version1 = 70;
    vid_t vid;
    CHECK(gi.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(id1), Property::from_int64(version1)}, vid));
    int64_t id2 = 2 * i;
    int64_t version2 = 80;
    CHECK(gi.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(id2), Property::from_int64(version2)}, vid));
  }
  txn.Commit();
  return svc;
}

void WS1(neug::NeugDBSession& db, int64_t person1_id, int64_t person2_id,
         std::mt19937& gen) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  vid_t person1_vid;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  bool flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person1_id) {
      person1_vid = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  int64_t p1_value =
      gui.GetVertexProperty(person_label_id, person1_vid, 1).as_int64();
  vid_t person2_vid;
  flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person2_id) {
      person2_vid = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  int64_t p2_value =
      gui.GetVertexProperty(person_label_id, person2_vid, 1).as_int64();

  if (p1_value + p2_value - 100 < 0) {
    txn.Abort();
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  std::uniform_int_distribution<> dist(0, 1);

  // pick randomly between person1 and person2 and decrement the value
  // property
  if (dist(gen)) {
    gui.UpdateVertexProperty(person_label_id, person1_vid, 1,
                             neug::Property::From(p1_value - 100));
  } else {
    gui.UpdateVertexProperty(person_label_id, person2_vid, 1,
                             neug::Property::From(p2_value - 100));
  }
  txn.Commit();
}

std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> WS2(
    neug::NeugDBSession& db) {
  std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> results;
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.graph(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto person_prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto vertex_set = gi.GetVertexSet(person_label_id);

  for (vid_t lid : vertex_set) {
    auto person1_id = person_prop_col->get(lid).as_int64();
    if (person1_id % 2 != 1) {
      continue;
    }
    int64_t p1_value = person_prop_col->get(lid).as_int64();
    auto person2_id = person1_id + 1;
    vid_t lid2;
    for (vid_t lid : vertex_set) {
      if (person_prop_col->get(lid).as_int64() == person2_id) {
        lid2 = lid;
        break;
      }
    }
    int64_t p2_value = person_prop_col->get(lid2).as_int64();
    if (p1_value + p2_value <= 0) {
      results.emplace_back(person1_id, p1_value, person2_id, p2_value);
    }
  }

  return results;
}

TEST_F(NeugDBACIDTest, AtomicityC) {
  std::string dir = work_dir_ + "/AtomicityC";
  NeugDB db;
  std::shared_ptr<neug::NeugDBService> svc =
      neug_AtomicityInit(db, dir, thread_num_);
  auto committed = neug_AtomicityCheck(svc);
  std::atomic<int> num_aborted_txns(0), num_committed_txns(0);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& session, int txn_id) {
        bool successful =
            neug_AtomicityC(session, 3 + txn_id, "alice@otherdomain.net", 2020);
        if (successful)
          num_committed_txns.fetch_add(1);
        else
          num_aborted_txns.fetch_add(1);
      },
      50);
  committed.first += num_committed_txns.load();
  committed.second += num_committed_txns.load();
  auto finalstate = neug_AtomicityCheck(svc);
  ASSERT_EQ(committed, finalstate);
}

TEST_F(NeugDBACIDTest, AtomicityRB) {
  std::string dir = work_dir_ + "/AtomicityRB";
  NeugDB db;
  auto svc = neug_AtomicityInit(db, dir, thread_num_);
  auto committed = neug_AtomicityCheck(svc);
  std::atomic<int> num_aborted_txns(0), num_committed_txns(0);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& session, int txn_id) {
        bool successful;
        if (txn_id % 2 == 0) {
          successful =
              neug_AtomicityRB(session, 2, "alice@otherdomain.net", 2020);
        } else {
          successful = neug_AtomicityRB(session, 3 + txn_id,
                                        "alice@otherdomain.net", 2020);
        }
        if (successful) {
          num_committed_txns.fetch_add(1);
        } else {
          num_aborted_txns.fetch_add(1);
        }
      },
      50);
  committed.first += num_committed_txns.load();
  committed.second += num_committed_txns.load();
  auto finalstate = neug_AtomicityCheck(svc);
  ASSERT_EQ(committed, finalstate);
}

// --- G0 (Dirty Writes) ---
TEST_F(NeugDBACIDTest, G0) {
  std::string dir = work_dir_ + "/G0";
  NeugDB db;
  auto svc = G0Init(db, dir, thread_num_);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& db, int txn_id) {
        std::random_device rand_dev;
        std::mt19937 gen(rand_dev());
        std::uniform_int_distribution<int> dist(1, 100);
        int picked = dist(gen) * 2 - 1;
        G0(db, picked, picked + 1, txn_id + 1);
      },
      200);
  std::string p1_version_history, p2_version_history, k_version_history;
  std::tie(p1_version_history, p2_version_history, k_version_history) =
      G0Check(db, svc, 1, 2);
  ASSERT_EQ(p1_version_history, p2_version_history);
  ASSERT_EQ(p2_version_history, k_version_history);
}

// --- G1A (Aborted Reads) ---
TEST_F(NeugDBACIDTest, G1A) {
  std::string dir = work_dir_ + "/G1A";
  NeugDB db;
  auto svc = G1AInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& db, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        auto p_version = G1A2(db);
        if (p_version != 1)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        G1A1(db);
      }
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- G1B (Intermediate Reads) ---
TEST_F(NeugDBACIDTest, G1B) {
  std::string dir = work_dir_ + "/G1B";
  NeugDB db;
  auto svc = G1BInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        auto p_version = G1B2(session);
        if (p_version % 2 != 1)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        G1B1(session, 0, 1);
      }
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- G1C (Circular Information Flow) ---
TEST_F(NeugDBACIDTest, G1C) {
  std::string dir = work_dir_ + "/G1C";
  NeugDB db;
  auto svc = G1CInit(db, dir, thread_num_);
  int64_t c = 1000;
  std::vector<int64_t> results(c);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& session, int txn_id) {
        std::random_device rand_dev;
        std::mt19937 gen(rand_dev());
        std::uniform_int_distribution<int> dist(1, 100);
        int64_t person1_id = dist(gen);
        int64_t person2_id;
        do {
          person2_id = dist(gen);
        } while (person1_id == person2_id);
        results[txn_id] = G1C(session, person1_id, person2_id, txn_id + 1);
      },
      c);
  int64_t num_incorrect_checks = 0;
  for (int64_t i = 1; i <= c; i++) {
    auto v1 = results[i - 1];
    if (v1 == 0)
      continue;
    auto v2 = results[v1 - 1];
    if (v2 == -1 || i == v2)
      num_incorrect_checks++;
  }
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- IMP (Item-Many-Preceders) ---
TEST_F(NeugDBACIDTest, IMP) {
  std::string dir = work_dir_ + "/IMP";
  NeugDB db;
  auto svc = IMPInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    if (client_id < rc) {
      std::random_device rand_dev;
      std::mt19937 gen(rand_dev());
      std::uniform_int_distribution<int> dist(1, 100);
      for (int i = 0; i < 100; ++i) {
        int picked = dist(gen);
        int64_t v1, v2;
        std::tie(v1, v2) = IMP2(session, picked);
        if (v1 != v2)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        IMP1(session);
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- PMP (Predicate-Many-Preceders) ---
TEST_F(NeugDBACIDTest, PMP) {
  std::string dir = work_dir_ + "/PMP";
  NeugDB db;
  auto svc = PMPInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  std::atomic<int64_t> num_aborted_txns(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        int64_t v1, v2;
        int post_id = dist(gen);
        std::tie(v1, v2) = PMP2(session, post_id);
        if (v1 != v2)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        int person_id = dist(gen);
        int post_id = dist(gen);
        if (!PMP1(session, person_id, post_id))
          num_aborted_txns.fetch_add(1);
      }
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- OTV (Observed Transaction Vanishes) ---
TEST_F(NeugDBACIDTest, OTV) {
  std::string dir = work_dir_ + "/OTV";
  NeugDB db;
  auto svc = OTVInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = OTV2(session, dist(gen) * 4 + 1);
        int64_t v1_max = std::max({std::get<0>(tup1), std::get<1>(tup1),
                                   std::get<2>(tup1), std::get<3>(tup1)});
        int64_t v2_min = std::min({std::get<0>(tup2), std::get<1>(tup2),
                                   std::get<2>(tup2), std::get<3>(tup2)});
        if (v1_max > v2_min)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        OTV1(session, dist(gen) * 4 + 1);
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- FR (Fractured Reads) ---
TEST_F(NeugDBACIDTest, FR) {
  std::string dir = work_dir_ + "/FR";
  NeugDB db;
  auto svc = FRInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = FR2(session, dist(gen) * 4 + 1);
        if (tup1 != tup2)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        FR1(session, dist(gen) * 4 + 1);
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- LU (Lost Updates) ---
TEST_F(NeugDBACIDTest, LU) {
  std::string dir = work_dir_ + "/LU";
  NeugDB db;
  auto svc = LUInit(db, dir, thread_num_);
  std::map<int64_t, int64_t> expNumFriends;
  std::mutex mtx;
  std::atomic<int64_t> num_aborted_txns(0);
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    std::map<int64_t, int64_t> localExpNumFriends;
    for (int i = 0; i < 100; ++i) {
      int64_t person_id = dist(gen);
      if (LU1(session, person_id))
        ++localExpNumFriends[person_id];
      else
        num_aborted_txns.fetch_add(1);
    }
    std::lock_guard<std::mutex> lock(mtx);
    for (auto& pair : localExpNumFriends)
      expNumFriends[pair.first] += pair.second;
  });
  auto sess = svc->AcquireSession();
  std::map<int64_t, int64_t> numFriends = LU2(*sess.get());
  ASSERT_EQ(numFriends, expNumFriends);
}

// --- WS (Write Skews) ---
TEST_F(NeugDBACIDTest, WS) {
  std::string dir = work_dir_ + "/WS";
  NeugDB db;
  auto svc = WSInit(db, dir, thread_num_);
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    for (int i = 0; i < 100; ++i) {
      int64_t person1_id = dist(gen) * 2 - 1;
      int64_t person2_id = person1_id + 1;
      WS1(session, person1_id, person2_id, gen);
    }
  });
  auto sess = svc->AcquireSession();
  auto results = WS2(*sess.get());
  ASSERT_TRUE(results.empty());
}