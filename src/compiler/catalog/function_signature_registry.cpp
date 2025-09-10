#include "neug/compiler/catalog/function_signature_registry.h"
#include "neug/compiler/common/types/types.h"

namespace gs {
namespace function {

void FunctionSignatureRegistry::registerScalar(const std::string& signature, 
                                               const runtime::neug_func_exec_t& func) {
    auto& inst = instance();
    std::unique_lock lk(inst.mutex_);
    inst.sig_2_scalar_func_[signature] = func;
}

runtime::neug_func_exec_t FunctionSignatureRegistry::lookup(const std::string& signature) {
    auto& inst = instance();
    std::shared_lock lk(inst.mutex_);
    auto it = inst.sig_2_scalar_func_.find(signature);
    return it == inst.sig_2_scalar_func_.end() ? nullptr : it->second;
}

FunctionSignatureRegistry& FunctionSignatureRegistry::instance() {
    static FunctionSignatureRegistry inst;
    return inst;
}

void FunctionSignatureRegistry::printAllSignatures() {
    auto& inst = instance();
    std::shared_lock lk(inst.mutex_);
    std::cout << "FunctionSignatureRegistry: sig_2_scalar_func_ contains:" << std::endl;
    for (const auto& kv : inst.sig_2_scalar_func_) {
        std::cout << "  " << kv.first << " -> " << (kv.second ? "valid_func" : "nullptr") << std::endl;
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