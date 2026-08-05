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

#include "neug/main/neug_db.h"

#include <glog/logging.h>

#include <brpc/channel.h>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

int submit_query(brpc::Channel& channel, const std::string& uri, int thread_id,
                 int query_num, const std::vector<std::string>& queries) {
  // A json format payload
  std::string payload = "{\"query\": \"" + queries[0] + "\"}";
  LOG(INFO) << "Thread " << thread_id << " starting with payload: " << payload;

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < query_num; ++i) {
    // We will receive response synchronously, safe to put variables on stack.
    brpc::Controller cntl;

    cntl.http_request().uri() = uri.c_str();
    cntl.request_attachment().append(payload);
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);

    auto req_start = std::chrono::steady_clock::now();
    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    auto req_end = std::chrono::steady_clock::now();

    if (cntl.Failed()) {
      LOG(ERROR) << "Thread " << thread_id << " request " << i
                 << " failed: " << cntl.ErrorText();
      return -1;
    }
    int status_code = cntl.http_response().status_code();
    const std::string* content_type =
        cntl.http_response().GetHeader("Content-Type");
    std::string response_body = cntl.response_attachment().to_string();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           req_end - req_start)
                           .count();

    LOG(INFO) << "Thread " << thread_id << " request " << i << " completed in "
              << duration_ms << "ms"
              << ", status=" << status_code
              << ", content_type=" << (content_type ? *content_type : "none")
              << ", body_size=" << response_body.size();
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

  LOG(INFO) << "Starting test with " << thread_num << " threads, "
            << request_num << " requests per thread";
  LOG(INFO) << "Target URI: " << uri;

  std::vector<std::thread> threads;
  std::vector<std::string> queries = {"MATCH (n)-[e]-(m) return count(e);"};

  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.protocol = "http";
  options.timeout_ms = 100000 /*milliseconds*/;
  options.connection_type = "pooled";
  options.max_retry = 0;  // No retry for testing concurrency

  if (channel.Init(uri.c_str(), "", &options) != 0) {
    LOG(ERROR) << "Fail to initialize channel";
    return -1;
  }

  auto test_start = std::chrono::steady_clock::now();

  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&, i]() { submit_query(channel, uri, i, request_num, queries); });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto test_end = std::chrono::steady_clock::now();
  auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      test_end - test_start)
                      .count();

  LOG(INFO) << "All threads completed. Total time: " << total_ms << "ms";
  LOG(INFO) << "Total requests: " << (thread_num * request_num);
  LOG(INFO) << "Average time per request: " << (total_ms / request_num) << "ms";

  return 0;
}