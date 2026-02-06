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
class RecordAccessor : public ExprBase {
 public:
  RecordAccessor(int tag, const DataType& type) : tag_(tag), type_(type) {}
  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  std::string name() const override {
    return "RecordAccessor[tag=" + std::to_string(tag_) + "]";
  }

 private:
  int tag_;
  DataType type_;
};

class RecordVertexAccessor : public ExprBase {
 public:
  RecordVertexAccessor(int tag, const DataType& data_type,
                       GraphAccessType access_type,
                       const std::string& property_name)
      : tag_(tag),
        data_type_(data_type),
        access_type_(access_type),
        property_name_(property_name) {}

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  static std::unique_ptr<ExprBase> create_property_accessor(
      int tag, const DataType& data_type, const std::string& property_name) {
    return std::make_unique<RecordVertexAccessor>(
        tag, data_type, GraphAccessType::kProperty, property_name);
  }

  static std::unique_ptr<ExprBase> create_label_accessor(int tag) {
    return std::make_unique<RecordVertexAccessor>(
        tag, DataType(DataTypeId::kVarchar), GraphAccessType::kLabel, "");
  }

  static std::unique_ptr<ExprBase> create_gid_accessor(int tag) {
    return std::make_unique<RecordVertexAccessor>(tag, DataType::INT64,
                                                  GraphAccessType::kGid, "");
  }

  static std::unique_ptr<ExprBase> create_identity_accessor(int tag) {
    return std::make_unique<RecordVertexAccessor>(
        tag, DataType::VERTEX, GraphAccessType::kIdentity, "");
  }

  std::string name() const override {
    switch (access_type_) {
    case GraphAccessType::kGid:
      return "RecordVertexGidAccessor[tag=" + std::to_string(tag_) + "]";
    case GraphAccessType::kLabel:
      return "RecordVertexLabelAccessor[tag=" + std::to_string(tag_) + "]";
    case GraphAccessType::kProperty:
      return "RecordVertexPropertyAccessor[tag=" + std::to_string(tag_) +
             ", prop=" + property_name_ + "]";
    default:
      return "UnknownRecordVertexAccessor[tag=" + std::to_string(tag_) + "]";
    }
  }

  const DataType& type() const override { return data_type_; }

 private:
  int tag_;
  DataType data_type_;
  GraphAccessType access_type_;
  std::string property_name_;
};

class RecordEdgeAccessor : public ExprBase {
 public:
  RecordEdgeAccessor(int tag, const DataType& data_type,
                     GraphAccessType access_type,
                     const std::string& property_name)
      : tag_(tag),
        data_type_(data_type),
        access_type_(access_type),
        property_name_(property_name) {}

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  static std::unique_ptr<ExprBase> create_property_accessor(
      int tag, const DataType& data_type, const std::string& property_name) {
    return std::make_unique<RecordEdgeAccessor>(
        tag, data_type, GraphAccessType::kProperty, property_name);
  }

  static std::unique_ptr<ExprBase> create_label_accessor(int tag) {
    return std::make_unique<RecordEdgeAccessor>(
        tag, DataType(DataTypeId::kVarchar), GraphAccessType::kLabel, "");
  }

  static std::unique_ptr<ExprBase> create_identity_accessor(int tag) {
    return std::make_unique<RecordEdgeAccessor>(tag, DataType::EDGE,
                                                GraphAccessType::kIdentity, "");
  }

  static std::unique_ptr<ExprBase> create_gid_accessor(int tag) {
    return std::make_unique<RecordEdgeAccessor>(tag, DataType::INT64,
                                                GraphAccessType::kGid, "");
  }

  const DataType& type() const override { return data_type_; }

  std::string name() const override {
    switch (access_type_) {
    case GraphAccessType::kLabel:
      return "EdgeRecordLabelAccessor[tag=" + std::to_string(tag_) + "]";
    case GraphAccessType::kProperty:
      return "EdgeRecordPropertyAccessor[tag=" + std::to_string(tag_) +
             ", prop=" + property_name_ + "]";
    case GraphAccessType::kGid:
      return "EdgeRecordGidAccessor[tag=" + std::to_string(tag_) + "]";
    default:
      return "UnknownEdgeRecordAccessor[tag=" + std::to_string(tag_) + "]";
    }
  }

 private:
  int tag_;
  DataType data_type_;
  GraphAccessType access_type_;
  std::string property_name_;
};

class RecordPathAccessor : public ExprBase {
 public:
  RecordPathAccessor(int tag, const DataType& type, const std::string& property)
      : tag_(tag), type_(type), property_(property) {}

  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

  std::string name() const override {
    return "RecordPathAccessor[tag=" + std::to_string(tag_) +
           ", property=" + property_ + "]";
  }

 private:
  int tag_;
  DataType type_;
  std::string property_;
};
}  // namespace execution
}  // namespace neug