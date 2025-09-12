/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include "neug/utils/function_type.h"
#include "neug/compiler/common/types/types.h"
#include <iostream>

namespace gs {
namespace function {

class FunctionSignatureRegistry {
public:
    static void registerScalar(const std::string& signature, const runtime::neug_func_exec_t& func);
    static runtime::neug_func_exec_t lookup(const std::string& signature);
    static void printAllSignatures();

private:
    static FunctionSignatureRegistry& instance();
    std::unordered_map<std::string, runtime::neug_func_exec_t> sig_2_scalar_func_;
    std::shared_mutex mutex_;
};

// generate function signature
std::string buildScalarSignature(const std::string& name,
                                 const std::vector<common::LogicalTypeID>& params);

}
}