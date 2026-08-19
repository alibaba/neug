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

#include "vector_distance_function.h"

#include <cmath>
#include <memory>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/utils/exception/exception.h"

namespace neug::vector_search_ext {
namespace {

std::unique_ptr<function::FunctionBindData> BindVectorDistance(
    const function::ScalarBindFuncInput& input) {
  if (input.arguments.size() != 2) {
    THROW_BINDER_EXCEPTION("Vector distance functions require two arguments");
  }

  const auto& lhs_type = input.arguments[0]->getDataType();
  if (lhs_type.id() != DataTypeId::kArray) {
    THROW_BINDER_EXCEPTION(
        "The first argument to a vector distance function must be an ARRAY");
  }

  auto element_type = ArrayType::GetChildType(lhs_type).id();
  if (element_type != DataTypeId::kFloat &&
      element_type != DataTypeId::kDouble) {
    THROW_BINDER_EXCEPTION(
        "Vector distance functions support only FLOAT or DOUBLE arrays");
  }

  std::vector<DataType> parameter_types;
  parameter_types.push_back(lhs_type.copy());
  parameter_types.push_back(lhs_type.copy());
  return std::make_unique<function::FunctionBindData>(
      std::move(parameter_types), DataType::DOUBLE);
}

const std::vector<Value>& ValidateArguments(const std::vector<Value>& args) {
  if (args[0].type().id() != DataTypeId::kArray ||
      args[1].type().id() != DataTypeId::kArray) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vector distance arguments must be arrays");
  }
  if (args[0].type() != args[1].type()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vector distance arguments must have the same array type");
  }

  const auto& lhs = ArrayValue::GetChildren(args[0]);
  const auto& rhs = ArrayValue::GetChildren(args[1]);
  if (lhs.size() != rhs.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vector distance arguments must have the same dimension");
  }
  return lhs;
}

template <typename T>
double L2Distance(const std::vector<Value>& lhs,
                  const std::vector<Value>& rhs) {
  double distance = 0.0;
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i].IsNull() || rhs[i].IsNull()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Vector distance arguments cannot contain null elements");
    }
    const double difference = static_cast<double>(lhs[i].GetValue<T>()) -
                              static_cast<double>(rhs[i].GetValue<T>());
    distance += difference * difference;
  }
  return distance;
}

template <typename T>
double CosineDistance(const std::vector<Value>& lhs,
                      const std::vector<Value>& rhs) {
  double dot_product = 0.0;
  double lhs_norm = 0.0;
  double rhs_norm = 0.0;
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i].IsNull() || rhs[i].IsNull()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Vector distance arguments cannot contain null elements");
    }
    const double lhs_value = static_cast<double>(lhs[i].GetValue<T>());
    const double rhs_value = static_cast<double>(rhs[i].GetValue<T>());
    dot_product += lhs_value * rhs_value;
    lhs_norm += lhs_value * lhs_value;
    rhs_norm += rhs_value * rhs_value;
  }
  if (lhs_norm == 0.0 || rhs_norm == 0.0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Cosine distance is undefined for a zero vector");
  }
  return 1.0 - dot_product / std::sqrt(lhs_norm * rhs_norm);
}

template <typename T>
double InnerProduct(const std::vector<Value>& lhs,
                    const std::vector<Value>& rhs) {
  double inner_product = 0.0;
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i].IsNull() || rhs[i].IsNull()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Vector distance arguments cannot contain null elements");
    }
    inner_product += static_cast<double>(lhs[i].GetValue<T>()) *
                     static_cast<double>(rhs[i].GetValue<T>());
  }
  return inner_product;
}

function::function_set MakeFunctionSet(execution::neug_func_exec_t exec) {
  function::function_set functions;
  auto function = std::make_unique<function::NeugScalarFunction>(
      "", std::vector<DataTypeId>{DataTypeId::kArray, DataTypeId::kArray},
      DataTypeId::kDouble, exec);
  function->bindFunc = BindVectorDistance;
  functions.push_back(std::move(function));
  return functions;
}

enum class VectorOperation { kL2, kCosine, kInnerProduct };

template <VectorOperation operation>
Value Execute(const std::vector<Value>& args) {
  if (args.size() != 2) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vector distance functions require two arguments");
  }
  if (args[0].IsNull() || args[1].IsNull()) {
    return Value(DataType::DOUBLE);
  }
  const auto& lhs = ValidateArguments(args);
  const auto& rhs = ArrayValue::GetChildren(args[1]);
  auto element_type = ArrayType::GetChildType(args[0].type()).id();
  if (element_type == DataTypeId::kFloat) {
    if constexpr (operation == VectorOperation::kCosine) {
      return Value::DOUBLE(CosineDistance<float>(lhs, rhs));
    } else if constexpr (operation == VectorOperation::kInnerProduct) {
      return Value::DOUBLE(InnerProduct<float>(lhs, rhs));
    } else {
      return Value::DOUBLE(L2Distance<float>(lhs, rhs));
    }
  }
  if (element_type == DataTypeId::kDouble) {
    if constexpr (operation == VectorOperation::kCosine) {
      return Value::DOUBLE(CosineDistance<double>(lhs, rhs));
    } else if constexpr (operation == VectorOperation::kInnerProduct) {
      return Value::DOUBLE(InnerProduct<double>(lhs, rhs));
    } else {
      return Value::DOUBLE(L2Distance<double>(lhs, rhs));
    }
  }
  THROW_INVALID_ARGUMENT_EXCEPTION(
      "Vector distance functions support only FLOAT or DOUBLE arrays");
}

}  // namespace

function::function_set VectorDistanceL2Function::getFunctionSet() {
  auto functions = MakeFunctionSet(VectorDistanceL2Function::Exec);
  functions[0]->name = name;
  return functions;
}

Value VectorDistanceL2Function::Exec(const std::vector<Value>& args) {
  return Execute<VectorOperation::kL2>(args);
}

function::function_set VectorDistanceCosineFunction::getFunctionSet() {
  auto functions = MakeFunctionSet(VectorDistanceCosineFunction::Exec);
  functions[0]->name = name;
  return functions;
}

Value VectorDistanceCosineFunction::Exec(const std::vector<Value>& args) {
  return Execute<VectorOperation::kCosine>(args);
}

function::function_set VectorDistanceIPFunction::getFunctionSet() {
  auto functions = MakeFunctionSet(VectorDistanceIPFunction::Exec);
  functions[0]->name = name;
  return functions;
}

Value VectorDistanceIPFunction::Exec(const std::vector<Value>& args) {
  return Execute<VectorOperation::kInnerProduct>(args);
}

}  // namespace neug::vector_search_ext
