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

// Include httplib.h before any project header on Windows. httplib.h pulls in
// winsock2.h; if a project header (e.g. neug/main/neug_db.h) includes windows.h
// first, the older winsock.h gets loaded and causes redefinition errors with
// winsock2.h.
#include "httplib.h"

#include "neug/main/neug_db.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

int submit_query(const std::string& base_url, int thread_id, int query_num,
                 const std::vector<std::string>& queries) {
  // A json format payload
  std::string payload = "{\"query\": \"" + queries[0] + "\"}";
  LOG(INFO) << "Thread " << thread_id << " starting with payload: " << payload;

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < query_num; ++i) {
    httplib::Client cli(base_url.c_str());
    cli.set_connection_timeout(100, 0);  // 100 seconds
    cli.set_read_timeout(100, 0);
    cli.set_write_timeout(100, 0);

    auto req_start = std::chrono::steady_clock::now();
    auto res = cli.Post("/cypher", payload, "application/json");
    auto req_end = std::chrono::steady_clock::now();

    if (!res) {
      LOG(ERROR) << "Thread " << thread_id << " request " << i
                 << " failed: no response";
      return -1;
    }
    int status_code = res->status;
    std::string content_type;
    if (res->has_header("Content-Type")) {
      content_type = res->get_header_value("Content-Type");
    } else {
      content_type = "none";
    }
    std::string response_body = res->body;
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           req_end - req_start)
                           .count();

    LOG(INFO) << "Thread " << thread_id << " request " << i << " completed in "
              << duration_ms << "ms"
              << ", status=" << status_code << ", content_type=" << content_type
              << ", body_size=" << response_body.size();

    // A non-2xx status is a failed request: remember it so the process exit
    // code reflects it instead of silently reporting success.
    if (status_code < 200 || status_code >= 300) {
      LOG(ERROR) << "Thread " << thread_id << " request " << i
                 << " failed: HTTP status " << status_code
                 << ", body=" << response_body;
      return 1;
    }
  }

  auto end = std::chrono::steady_clock::now();
  auto total_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  LOG(INFO) << "Thread " << thread_id << " finished " << query_num
            << " requests in " << total_ms << "ms";

  return 0;
}

int main(int argc, char** argv) {
  if (argc != 4) {
    LOG(ERROR) << "Usage: " << argv[0] << " <uri> <thread_num> <request_num>";
    return -1;
  }
  std::string uri = argv[1];
  int thread_num = atoi(argv[2]);
  int request_num = atoi(argv[3]);
  // Reject nonpositive counts before allocating worker vectors or dividing:
  // atoi also turns non-numeric input into 0, which would divide by zero
  // below; a zero thread count would report success without sending any
  // request; a negative count would create a huge (or invalid) vector.
  if (thread_num <= 0 || request_num <= 0) {
    LOG(ERROR) << "thread_num and request_num must be positive integers, got: "
               << thread_num << " threads, " << request_num
               << " requests per thread";
    return -1;
  }

  // Strip "http://" prefix if present for base_url
  std::string base_url = uri;
  const std::string prefix = "http://";
  if (base_url.find(prefix) == 0) {
    base_url = base_url.substr(prefix.size());
  }

  LOG(INFO) << "Starting test with " << thread_num << " threads, "
            << request_num << " requests per thread";
  LOG(INFO) << "Target URI: " << uri;

  std::vector<std::thread> threads;
  std::vector<std::string> queries = {"MATCH (n)-[e]-(m) return count(e);"};
  std::vector<int> results(thread_num, 0);

  auto test_start = std::chrono::steady_clock::now();

  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([&, i]() {
      results[i] = submit_query(base_url, i, request_num, queries);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto test_end = std::chrono::steady_clock::now();
  auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      test_end - test_start)
                      .count();

  LOG(INFO) << "All threads completed. Total time: " << total_ms << "ms";
  const int total_requests = thread_num * request_num;
  LOG(INFO) << "Total requests: " << total_requests;
  LOG(INFO) << "Average time per request: " << (total_ms / total_requests)
            << "ms";

  const int failed_threads = std::count_if(results.begin(), results.end(),
                                           [](int r) { return r != 0; });
  if (failed_threads > 0) {
    LOG(ERROR) << failed_threads << " of " << thread_num
               << " worker threads reported failures";
    return 1;
  }
  return 0;
}
