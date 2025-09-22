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


#include "glog/logging.h"
#include "neug/utils/exception/exception.h"
#include "neug/compiler/extension/extension_api.h"

// JSON functions
#include "include/json_dummy_function.h"

extern "C" {

void init() {
    LOG(INFO) << "[json extension] init called";

    try {
        gs::extension::ExtensionAPI::registerScalarFunction<gs::extension::JsonDummyFunction>();
        LOG(INFO) << "[json extension] functions registered successfully";
    } catch (const std::exception& e) {
        THROW_EXCEPTION_WITH_FILE_LINE("[json extension] registration failed");
    }
}

const char* name() {
    return "JSON";
}

} // extern "C"
