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

#pragma once
#include "neug/execution/expression/accessors/common.h"
#include "neug/execution/expression/expr.h"

namespace neug {
namespace execution {
class VertexAccessor : public ExprBase {
 public:
  VertexAccessor(const DataType& type, GraphAccessType access_type,
                 const std::string& property_name)
      : type_(type), access_type_(access_type), property_name_(property_name) {}

  const DataType& type() const override { return type_; }

  static std::unique_ptr<ExprBase> create_property_accessor(
      const DataType& type, const std::string& property_name) {
    return std::make_unique<VertexAccessor>(type, GraphAccessType::kProperty,
                                            property_name);
  }

  static std::unique_ptr<ExprBase> create_label_accessor() {
    return std::make_unique<VertexAccessor>(DataType(DataTypeId::kVarchar),
                                            GraphAccessType::kLabel, "");
  }

  static std::unique_ptr<ExprBase> create_gid_accessor() {
    return std::make_unique<VertexAccessor>(DataType::INT64,
                                            GraphAccessType::kGid, "");
  }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  std::string name() const override {
    switch (access_type_) {
    case GraphAccessType::kGid:
      return "VertexGidAccessor";
    case GraphAccessType::kLabel:
      return "VertexLabelAccessor";
    case GraphAccessType::kProperty:
      return std::string("VertexPropertyAccessor[") + property_name_ + "]";
    default:
      return "UnknownVertexAccessor";
    }
  }

 private:
  DataType type_;
  GraphAccessType access_type_;
  std::string property_name_;
};
}  // namespace execution
}  // namespace neug