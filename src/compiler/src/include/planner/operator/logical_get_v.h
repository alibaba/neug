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

#include <memory>
#include <string>
#include <vector>
#include "binder/expression/expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "gopt/g_alias_name.h"
#include "gopt/g_constants.h"
#include "gopt/g_graph_type.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/schema.h"

namespace gs {
namespace planner {

enum GetVOpt { START = 0, END = 1, OTHER = 2, BOTH = 3, ITSELF = 4 };

struct GNodeType;

class LogicalGetV : public LogicalOperator {
 public:
  LogicalGetV(std::shared_ptr<binder::Expression> nodeID,
              std::vector<common::table_id_t> nodeTableIDs,
              binder::expression_vector properties, GetVOpt opt,
              std::shared_ptr<binder::RelExpression> boundRel,
              std::shared_ptr<LogicalOperator> child,
              std::unique_ptr<Schema> schema,
              common::cardinality_t cardinality = 0)
      : LogicalOperator{LogicalOperatorType::GET_V, child, cardinality},
        nodeID{std::move(nodeID)},
        nodeTableIDs{std::move(nodeTableIDs)},
        properties{std::move(properties)},
        boundRel{std::move(boundRel)},
        opt{opt} {
    this->schema = std::move(schema);
  }

  std::string getExpressionsForPrinting() const override;
  void computeFlatSchema() override;
  virtual void computeFactorizedSchema();
  std::unique_ptr<LogicalOperator> copy() override;

  // Getters
  inline std::shared_ptr<binder::Expression> getNodeID() const {
    return nodeID;
  }

  inline const binder::expression_vector& getProperties() const {
    return properties;
  }

  inline std::vector<common::table_id_t> getTableIDs() const {
    return nodeTableIDs;
  }

  inline GetVOpt getGetVOpt() const { return opt; }

  inline void setGetVOpt(GetVOpt opt_) { opt = opt_; }

  std::string getAliasName() const;
  gopt::GAliasName getGAliasName() const;
  std::unique_ptr<gopt::GNodeType> getNodeType(catalog::Catalog* catalog) const;

  inline std::shared_ptr<binder::Expression> setPredicates(
      std::shared_ptr<binder::Expression> predicates_) {
    predicates = std::move(predicates_);
    return predicates;
  }

  inline std::shared_ptr<binder::Expression> getPredicates() const {
    return predicates;
  }

 private:
  std::shared_ptr<binder::Expression> nodeID;
  std::vector<common::table_id_t> nodeTableIDs;
  binder::expression_vector properties;
  std::shared_ptr<binder::RelExpression> boundRel;
  GetVOpt opt;
  std::shared_ptr<binder::Expression> predicates;
};

}  // namespace planner
}  // namespace gs