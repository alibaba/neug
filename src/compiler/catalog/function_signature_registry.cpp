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

#include "neug/compiler/catalog/function_signature_registry.h"
#include "neug/compiler/common/types/types.h"

namespace gs {
namespace function {

void FunctionSignatureRegistry::registerScalar(const std::string& signature, 
                                               const runtime::neug_func_exec_t& func) {
    auto& inst = instance();
    std::unique_lock lk(inst.mutex_);
    auto it = inst.sig_2_scalar_func_.find(signature);
    if (it != inst.sig_2_scalar_func_.end()) {
        LOG(WARNING) << "registerScalar: signature [" << signature << "] already registered, skip.";
        return;
    }
    inst.sig_2_scalar_func_[signature] = func;
}

runtime::neug_func_exec_t FunctionSignatureRegistry::lookup(const std::string& signature) {
    auto& inst = instance();
    std::shared_lock lk(inst.mutex_);
    auto it = inst.sig_2_scalar_func_.find(signature);
    if (it == inst.sig_2_scalar_func_.end()) {
        LOG(ERROR) << "lookup: signature [" << signature << "] not found.";
        throw std::runtime_error("Signature not found");
    }
    return it->second;
}

FunctionSignatureRegistry& FunctionSignatureRegistry::instance() {
    static FunctionSignatureRegistry inst;
    return inst;
}

void FunctionSignatureRegistry::printAllSignatures() {
    auto& inst = instance();
    std::shared_lock lk(inst.mutex_);
    LOG(INFO) << "FunctionSignatureRegistry: sig_2_scalar_func_ contains:";
    for (const auto& kv : inst.sig_2_scalar_func_) {
        LOG(INFO) << "  " << kv.first << " -> " << (kv.second ? "valid_func" : "nullptr");
    }
}

std::string buildScalarSignature(const std::string& name,
                                 const std::vector<common::LogicalTypeID>& params) {
    std::string sig = name + "(";
    for (size_t i = 0; i < params.size(); ++i) {
        if (i) sig += ",";
        sig += common::LogicalTypeUtils::toString(params[i]);
    }
    sig += ")";
    return sig;
}

}
}