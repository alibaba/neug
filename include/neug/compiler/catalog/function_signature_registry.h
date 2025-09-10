#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include "neug/utils/neug_function_type.h"
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