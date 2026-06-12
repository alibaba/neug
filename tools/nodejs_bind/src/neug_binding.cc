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

#include <napi.h>

#include <iostream>
#include <string>

#include <glog/logging.h>

#include "neug/utils/exception/exception.h"
#include "node_connection.h"
#include "node_database.h"
#include "node_query_request.h"
#include "node_query_result.h"

namespace neug {

void SetupLogging() {
  google::InitGoogleLogging("neug");
  const char* debug = std::getenv("DEBUG");

  if (debug) {
    std::string mode = debug;
    if (mode == "1" || mode == "true" || mode == "ON") {
      FLAGS_minloglevel = 0;     // 0 for verbose
      FLAGS_logtostderr = true;  // Log to stderr
    } else {
      std::cerr << "Invalid DEBUG value: " << mode
                << ". Expected '1', 'true', or 'ON'." << std::endl;
      FLAGS_minloglevel = 2;      // 2 for error
      FLAGS_logtostderr = false;  // Log to file instead of stderr
    }
  } else {
    FLAGS_minloglevel = 2;      // 2 for error
    FLAGS_logtostderr = false;  // Log to file instead of stderr
  }
}

}  // namespace neug

static Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  neug::NodeDatabase::Init(env, exports);
  neug::NodeConnection::Init(env, exports);
  neug::NodeQueryResult::Init(env, exports);
  neug::NodeQueryRequest::Init(env, exports);

  neug::SetupLogging();

  return exports;
}

NODE_API_MODULE(neug_node_bind, InitAll)
