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

#include "src/engines/http_server/handler/http_utils.h"
#include <glog/logging.h>

namespace server {

seastar::future<std::unique_ptr<seastar::httpd::reply>> new_reply(
    std::unique_ptr<seastar::httpd::reply> rep,
    seastar::httpd::reply::status_type status, const std::string& msg) {
  rep->set_status(status);
  rep->set_content_type("application/json");
  rep->write_body("json", seastar::sstring(msg));
  rep->done();
  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
      std::move(rep));
}

seastar::future<std::unique_ptr<seastar::httpd::reply>> new_bad_request_reply(
    std::unique_ptr<seastar::httpd::reply> rep, const std::string& msg) {
  return new_reply(std::move(rep),
                   seastar::httpd::reply::status_type::bad_request, msg);
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
new_internal_error_reply(std::unique_ptr<seastar::httpd::reply> rep,
                         const std::string& msg) {
  rep->set_status(seastar::httpd::reply::status_type::bad_request);
  rep->set_content_type("application/json");
  gs::Status status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, msg);
  rep->write_body("json", seastar::sstring(status.ToString()));
  rep->done();
  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
      std::move(rep));
}

seastar::httpd::reply::status_type status_code_to_http_code(
    gs::StatusCode code) {
  switch (code) {
  case gs::StatusCode::OK:
    return seastar::httpd::reply::status_type::ok;
  case gs::StatusCode::ERR_INVALID_ARGUMENT:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::ERR_ILLEGAL_OPERATION:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::ERR_NOT_FOUND:
    return seastar::httpd::reply::status_type::not_found;
  case gs::StatusCode::ERR_CODEGEN_ERROR:
    return seastar::httpd::reply::status_type::internal_server_error;
  case gs::StatusCode::ERR_INVALID_SCHEMA:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::ERR_PERMISSION:
    return seastar::httpd::reply::status_type::forbidden;
  case gs::StatusCode::ERR_INTERNAL_ERROR:
    return seastar::httpd::reply::status_type::internal_server_error;
  case gs::StatusCode::ERR_IO_ERROR:
    return seastar::httpd::reply::status_type::internal_server_error;
  case gs::StatusCode::ERR_QUERY_EXECUTION:
    return seastar::httpd::reply::status_type::internal_server_error;
  default:
    return seastar::httpd::reply::status_type::internal_server_error;
  }
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
catch_exception_and_return_reply(std::unique_ptr<seastar::httpd::reply> rep,
                                 std::exception_ptr ex) {
  try {
    std::rethrow_exception(ex);
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    seastar::sstring what = e.what();
    rep->set_content_type("application/json");
    // for the exception, we are not sure whether it is a bad request or
    // internal server error
    gs::Status status = gs::Status(gs::StatusCode::ERR_UNKNOWN, what);
    rep->write_body("json", seastar::sstring(status.ToString()));
    rep->set_status(seastar::httpd::reply::status_type::internal_server_error);
    rep->done();
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
        std::move(rep));
  }
}

std::string trim_slash(const std::string& origin) {
  std::string res = origin;
  if (res.front() == '/') {
    res.erase(res.begin());
  }
  if (res.back() == '/') {
    res.pop_back();
  }
  return res;
}

}  // namespace server