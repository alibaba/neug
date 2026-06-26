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

#pragma once

#include <memory>
#include <string>

#include "neug/compiler/catalog/catalog_entry/catalog_entry.h"
#include "neug/compiler/optimizer/logical_rule.h"

namespace neug::catalog {

class NEUG_API RuleCatalogEntry final : public CatalogEntry {
 public:
  RuleCatalogEntry(std::string name,
                   std::unique_ptr<optimizer::LogicalRule> rule)
      : CatalogEntry{CatalogEntryType::RULE_ENTRY, std::move(name)},
        planRule{std::move(rule)} {}

  optimizer::LogicalRule* getRule() const { return planRule.get(); }

 private:
  std::unique_ptr<optimizer::LogicalRule> planRule;
};

}  // namespace neug::catalog
