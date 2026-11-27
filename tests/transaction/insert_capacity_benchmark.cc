/** Copyright 2026 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0.
 */
// Small synthetic LDBC-shaped write workload, NOT the LDBC driver/audit.
// Usage: insert_capacity_benchmark THREADS ITERATIONS reserved|growth DB_PATH
// Uses the service transaction entry point without HTTP/client overhead.
#include <atomic>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/transaction/insert_capacity.h"

int main(int argc, char** argv) {
  if (argc != 5)
    return 2;
  const int threads = std::stoi(argv[1]), iterations = std::stoi(argv[2]);
  const bool reserved = std::string(argv[3]) == "reserved";
  const std::string path = argv[4];
  if (threads < 1 || iterations < 1 || std::filesystem::exists(path))
    return 2;
  neug::NeugDB db;
  neug::NeugDBConfig config(path, threads);
  config.checkpoint_on_close = false;
  if (!db.Open(config))
    return 3;
  auto conn = db.Connect();
  auto query = [&](const std::string& q) {
    auto result = conn->Query(q);
    if (!result)
      throw std::runtime_error(result.error().ToString());
  };
  query("CREATE NODE TABLE Person(id INT64 PRIMARY KEY)");
  query("CREATE NODE TABLE Forum(id INT64 PRIMARY KEY)");
  query("CREATE NODE TABLE Comment(id INT64 PRIMARY KEY, content STRING)");
  query("CREATE REL TABLE Likes(FROM Person TO Comment, seq INT64)");
  query("CREATE REL TABLE Member(FROM Person TO Forum, seq INT64)");
  query("CREATE REL TABLE Creator(FROM Comment TO Person)");
  std::string ids = "[";
  for (int i = 0; i < 1000; ++i)
    ids += (i ? "," : "") + std::to_string(i);
  ids += "]";
  query("UNWIND " + ids + " AS id CREATE (:Person {id:id})");
  query("UNWIND " + ids + " AS id CREATE (:Forum {id:id})");
  query("UNWIND " + ids + " AS id CREATE (:Comment {id:id,content:'seed'})");
  const int warmup = 60;
  const int per_worker = iterations + warmup;
  if (reserved) {
    neug::InsertCapacityRequest capacity;
    for (int w = 0; w < threads; ++w) {
      for (int i = 0; i < per_worker; ++i) {
        auto id = neug::Value::INT64(1000 + w * per_worker + i);
        if (i % 3 == 0) {
          capacity.AddVertex("Comment", id,
                             {neug::Value::STRING(std::string(120, 'x'))});
          capacity.AddEdge("Comment", "Person", "Creator", {});
        } else if (i % 3 == 1) {
          capacity.AddEdge("Person", "Comment", "Likes", {id});
        } else {
          capacity.AddEdge("Person", "Forum", "Member", {id});
        }
      }
    }
    capacity.Grow(const_cast<neug::PropertyGraph&>(db.graph()));
  }
  conn->Close();
  db.PrepareForServing();
  std::atomic<int> errors{0}, retries{0};
  double seconds = 0;
  {
    neug::NeugDBService service(db);
    std::barrier ready(threads + 1), done(threads + 1);
    std::vector<std::thread> workers;
    for (int w = 0; w < threads; ++w)
      workers.emplace_back([&, w] {
        auto slot = service.AcquireExecutionSlot();
#ifdef NEUG_BUILD_TEST
        neug::SetInsertRetryHookForTest([&](bool retry) {
          if (!retry)
            ++retries;
        });
#endif
        for (int i = 0; i < per_worker; ++i) {
          if (i == warmup)
            ready.arrive_and_wait();
          const auto id = 1000 + w * per_worker + i;
          const std::string q =
              i % 3 == 0
                  ? "MATCH (p:Person {id:$p}) CREATE (c:Comment "
                    "{id:$id,content:$text})-[:Creator]->(p)"
                  : i % 3 == 1 ? "MATCH (p:Person {id:$p}), (c:Comment "
                                 "{id:$p}) CREATE (p)-[:Likes {seq:$id}]->(c)"
                               : "MATCH (p:Person {id:$p}), (f:Forum {id:$p}) "
                                 "CREATE (p)-[:Member {seq:$id}]->(f)";
          auto result = slot->ExecuteTransactionalRequest(
              "{\"query\":\"" + q +
              "\",\"access_mode\":\"insert\",\"parameters\":{\"id\":" +
              std::to_string(id) +
              ",\"p\":" + std::to_string((w * 37 + i) % 1000) + ",\"text\":\"" +
              std::string(120, 'x') + "\"}}");
          if (!result) {
            if (errors.fetch_add(1) == 0)
              std::cerr << result.error().ToString() << '\n';
          }
        }
        done.arrive_and_wait();
#ifdef NEUG_BUILD_TEST
        neug::SetInsertRetryHookForTest({});
#endif
      });
    ready.arrive_and_wait();
    const auto timed_start = std::chrono::steady_clock::now();
    done.arrive_and_wait();
    seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                            timed_start)
                  .count();
    for (auto& worker : workers)
      worker.join();
    auto slot = service.AcquireExecutionSlot();
    auto check = [&](const std::string& q, int64_t expected) {
      auto result = slot->ExecuteTransactionalRequest(
          "{\"query\":\"" + q +
          "\",\"access_mode\":\"read\",\"parameters\":{}}");
      if (!result ||
          neug::QueryResult::From(result.value()).GetInt64(0) != expected)
        ++errors;
    };
    check("MATCH (c:Comment) RETURN count(c)",
          1000 + threads * ((per_worker + 2) / 3));
    check("MATCH ()-[e:Creator]->() RETURN count(e)",
          threads * ((per_worker + 2) / 3));
    check("MATCH ()-[e:Likes]->() RETURN count(e)",
          threads * ((per_worker + 1) / 3));
    check("MATCH ()-[e:Member]->() RETURN count(e)",
          threads * (per_worker / 3));
    check("MATCH (c:Comment) WHERE c.id >= 1000 AND c.content = '" +
              std::string(120, 'x') + "' RETURN count(c)",
          threads * ((per_worker + 2) / 3));
  }
  db.Close();
  std::filesystem::remove_all(path);
  std::cout << "RESULT {\"threads\":" << threads
            << ",\"iterations\":" << iterations << ",\"mode\":\"" << argv[3]
            << "\",\"seconds\":" << seconds
            << ",\"ops_per_second\":" << threads * iterations / seconds
            << ",\"errors\":" << errors << ",\"capacity_retries\":" << retries
            << "}\n";
  return errors ? 1 : 0;
}
