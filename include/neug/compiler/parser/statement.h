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

#pragma once

#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/enums/statement_type.h"

namespace neug {
namespace parser {

class Statement {
 public:
  explicit Statement(common::StatementType statementType)
      : parsingTime{0}, statementType{statementType}, internal{false} {}

  virtual ~Statement() = default;

  common::StatementType getStatementType() const { return statementType; }
  void setToInternal() { internal = true; }
  bool isInternal() const { return internal; }
  void setParsingTime(double time) { parsingTime = time; }
  double getParsingTime() const { return parsingTime; }

  bool requireTransaction() const {
    switch (statementType) {
    case common::StatementType::TRANSACTION:
      return false;
    default:
      return true;
    }
  }

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

 private:
  double parsingTime;
  common::StatementType statementType;
  // By setting the statement to internal, we still execute the statement, but
  // will not return the executio result as part of the query result returned to
  // users. The use case for this is when a query internally generates other
  // queries to finish first, e.g., `TableFunction::rewriteFunc`.
  bool internal;
};

}  // namespace parser
}  // namespace neug
