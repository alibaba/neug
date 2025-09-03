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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/function/sequence/sequence_functions.h"

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/sequence_catalog_entry.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/main/client_context.h"

using namespace gs::common;

namespace gs {
namespace function {

struct CurrVal {
  static void operation(common::ku_string_t& input, common::ValueVector& result,
                        void* dataPtr) {
    auto ctx = reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext;
    auto catalog = ctx->getCatalog();
    auto sequenceName = input.getAsString();
    auto sequenceEntry = catalog->getSequenceEntry(
        ctx->getTransaction(), sequenceName, ctx->useInternalCatalogEntry());
    result.setValue(0, sequenceEntry->currVal());
  }
};

struct NextVal {
  static void operation(common::ku_string_t& input, common::ValueVector& result,
                        void* dataPtr) {
    auto ctx = reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext;
    auto cnt = reinterpret_cast<FunctionBindData*>(dataPtr)->count;
    auto catalog = ctx->getCatalog();
    auto sequenceName = input.getAsString();
    auto sequenceEntry = catalog->getSequenceEntry(
        ctx->getTransaction(), sequenceName, ctx->useInternalCatalogEntry());
    sequenceEntry->nextKVal(ctx->getTransaction(), cnt, result);
    result.state->getSelVectorUnsafe().setSelSize(cnt);
  }
};

function_set CurrValFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::INT64,
      ScalarFunction::UnarySequenceExecFunction<common::ku_string_t,
                                                common::ValueVector, CurrVal>));
  return functionSet;
}

function_set NextValFunction::getFunctionSet() {
  function_set functionSet;
  auto func = make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::INT64,
      ScalarFunction::UnarySequenceExecFunction<common::ku_string_t,
                                                common::ValueVector, NextVal>);
  func->isReadOnly = false;
  functionSet.push_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
