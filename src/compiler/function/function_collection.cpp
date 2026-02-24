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

#include "neug/compiler/function/function_collection.h"

#include "neug/compiler/function/aggregate/count.h"
#include "neug/compiler/function/aggregate/count_star.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/compiler/function/cast/vector_cast_functions.h"
#include "neug/compiler/function/comparison/vector_comparison_functions.h"
#include "neug/compiler/function/csv_read_function.h"
#include "neug/compiler/function/date/vector_date_functions.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/path/vector_path_functions.h"
#include "neug/compiler/function/schema/vector_node_rel_functions.h"
#include "neug/compiler/function/sequence/sequence_functions.h"
#include "neug/compiler/function/show_loaded_extensions_function.h"
#include "neug/compiler/function/string/vector_string_functions.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"
#include "neug/compiler/function/uuid/vector_uuid_functions.h"

using namespace neug::processor;

namespace neug {
namespace function {

#define SCALAR_FUNCTION_BASE(_PARAM, _NAME) \
  { _PARAM::getFunctionSet, _NAME, CatalogEntryType::SCALAR_FUNCTION_ENTRY }
#define SCALAR_FUNCTION(_PARAM) SCALAR_FUNCTION_BASE(_PARAM, _PARAM::name)
#define SCALAR_FUNCTION_ALIAS(_PARAM) \
  SCALAR_FUNCTION_BASE(_PARAM::alias, _PARAM::name)
#define REWRITE_FUNCTION_BASE(_PARAM, _NAME) \
  { _PARAM::getFunctionSet, _NAME, CatalogEntryType::REWRITE_FUNCTION_ENTRY }
#define REWRITE_FUNCTION(_PARAM) REWRITE_FUNCTION_BASE(_PARAM, _PARAM::name)
#define REWRITE_FUNCTION_ALIAS(_PARAM) \
  REWRITE_FUNCTION_BASE(_PARAM::alias, _PARAM::name)
#define AGGREGATE_FUNCTION(_PARAM)                 \
  {                                                \
    _PARAM::getFunctionSet, _PARAM::name,          \
        CatalogEntryType::AGGREGATE_FUNCTION_ENTRY \
  }
#define EXPORT_FUNCTION(_PARAM)               \
  {                                           \
    _PARAM::getFunctionSet, _PARAM::name,     \
        CatalogEntryType::COPY_FUNCTION_ENTRY \
  }
#define TABLE_FUNCTION(_PARAM)                 \
  {                                            \
    _PARAM::getFunctionSet, _PARAM::name,      \
        CatalogEntryType::TABLE_FUNCTION_ENTRY \
  }
#define STANDALONE_TABLE_FUNCTION(_PARAM)                 \
  {                                                       \
    _PARAM::getFunctionSet, _PARAM::name,                 \
        CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY \
  }
#define FINAL_FUNCTION \
  { nullptr, nullptr, CatalogEntryType::SCALAR_FUNCTION_ENTRY }

FunctionCollection* FunctionCollection::getFunctions() {
  static FunctionCollection functions[] = {

      SCALAR_FUNCTION(AddFunction),
      SCALAR_FUNCTION(SubtractFunction),
      SCALAR_FUNCTION(MultiplyFunction),
      SCALAR_FUNCTION(DivideFunction),
      SCALAR_FUNCTION(ModuloFunction),
      SCALAR_FUNCTION(PowerFunction),
      SCALAR_FUNCTION(AbsFunction),
      SCALAR_FUNCTION(NegateFunction),

      SCALAR_FUNCTION(ListCreationFunction),
      SCALAR_FUNCTION(ListExtractFunction),
      SCALAR_FUNCTION(ListContainsFunction),
      SCALAR_FUNCTION_ALIAS(ListHasFunction),

      SCALAR_FUNCTION(CastToDateFunction),
      SCALAR_FUNCTION_ALIAS(DateFunction),
      SCALAR_FUNCTION(CastToTimestampFunction),
      SCALAR_FUNCTION(CastToIntervalFunction),
      SCALAR_FUNCTION_ALIAS(IntervalFunctionAlias),
      SCALAR_FUNCTION_ALIAS(DurationFunction),
      SCALAR_FUNCTION(CastAnyFunction),

      SCALAR_FUNCTION(EqualsFunction),
      SCALAR_FUNCTION(NotEqualsFunction),
      SCALAR_FUNCTION(GreaterThanFunction),
      SCALAR_FUNCTION(GreaterThanEqualsFunction),
      SCALAR_FUNCTION(LessThanFunction),
      SCALAR_FUNCTION(LessThanEqualsFunction),

      SCALAR_FUNCTION(DatePartFunction),
      SCALAR_FUNCTION_ALIAS(DatePartFunctionAlias),
      SCALAR_FUNCTION(StructExtractFunctions),

      REWRITE_FUNCTION(IDFunction),
      REWRITE_FUNCTION(StartNodeFunction),
      REWRITE_FUNCTION(EndNodeFunction),
      REWRITE_FUNCTION(LabelFunction),
      REWRITE_FUNCTION_ALIAS(LabelsFunction),
      REWRITE_FUNCTION(CostFunction),

      SCALAR_FUNCTION(NodesFunction),
      SCALAR_FUNCTION(RelsFunction),
      SCALAR_FUNCTION_ALIAS(RelationshipsFunction),
      SCALAR_FUNCTION(PropertiesFunction),
      SCALAR_FUNCTION(IsTrailFunction),
      SCALAR_FUNCTION(IsACyclicFunction),
      REWRITE_FUNCTION(LengthFunction),

      AGGREGATE_FUNCTION(CountStarFunction),
      AGGREGATE_FUNCTION(CountFunction),
      AGGREGATE_FUNCTION(AggregateSumFunction),
      AGGREGATE_FUNCTION(AggregateAvgFunction),
      AGGREGATE_FUNCTION(AggregateMinFunction),
      AGGREGATE_FUNCTION(AggregateMaxFunction),
      AGGREGATE_FUNCTION(CollectFunction),

      SCALAR_FUNCTION(LowerFunction),
      SCALAR_FUNCTION_ALIAS(ToLowerFunction),
      SCALAR_FUNCTION_ALIAS(LcaseFunction),
      SCALAR_FUNCTION(UpperFunction),
      SCALAR_FUNCTION_ALIAS(UCaseFunction),
      SCALAR_FUNCTION_ALIAS(ToUpperFunction),
      SCALAR_FUNCTION(ContainsFunction),
      SCALAR_FUNCTION(EndsWithFunction),
      SCALAR_FUNCTION(StartsWithFunction),
      SCALAR_FUNCTION_ALIAS(SuffixFunction),
      SCALAR_FUNCTION(ReverseFunction),

      TABLE_FUNCTION(ShowLoadedExtensionsFunction),
      TABLE_FUNCTION(CSVReadFunction),

      FINAL_FUNCTION};

  return functions;
}

}  // namespace function
}  // namespace neug
