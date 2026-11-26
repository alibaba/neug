/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/server/httplib_transport.h"

#include <memory>

#include "default_service_transport.h"
#include "neug/server/service_config.h"

namespace neug {

std::unique_ptr<IServiceTransport> CreateDefaultServiceTransport(
    ITpOperations& service, const ServiceConfig& config,
    int database_thread_num) {
  const auto thread_num =
      config.thread_num != 0
          ? config.thread_num
          : static_cast<uint32_t>(database_thread_num > 0 ? database_thread_num
                                                          : 1);
  return std::make_unique<HttplibTransport>(service, config.host_str,
                                            config.query_port, thread_num);
}

}  // namespace neug
