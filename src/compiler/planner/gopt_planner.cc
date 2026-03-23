/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "neug/compiler/planner/gopt_planner.h"
#include <yaml-cpp/node/emit.h>
#include <cctype>
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_physical_convertor.h"
#include "neug/compiler/gopt/g_result_schema.h"
#include "neug/utils/exception/exception.h"

namespace neug {

result<std::pair<physical::PhysicalPlan, std::string>> GOptPlanner::compilePlan(
    const std::string& query) {
  VLOG(1) << "[GOptPlanner] compilePlan called with query: " << query;
  // read access to the planner

  if (database->getCatalog() == nullptr) {
    RETURN_ERROR(
        Status(StatusCode::ERR_INVALID_SCHEMA, "Catalog is not initialized"));
  }

  try {
    // Prepare and compile query
    auto statement = ctx->prepare(query);
    if (!statement->success) {
      RETURN_ERROR(Status(StatusCode::ERR_QUERY_SYNTAX, statement->errMsg));
    }

    VLOG(1) << "Logical Plan: " << std::endl
            << statement->logicalPlan->toString() << std::endl;

    if (statement->logicalPlan->emptyResult(
            statement->logicalPlan->getLastOperator())) {
      // If the logical plan results in an empty result,
      // return an empty physical plan.
      return std::make_pair(physical::PhysicalPlan(), std::string(""));
    }

    auto aliasManager =
        std::make_shared<neug::gopt::GAliasManager>(*statement->logicalPlan);
    neug::gopt::GPhysicalConvertor converter(aliasManager,
                                             database->getCatalog());
    auto physicalPlan = converter.convert(*statement->logicalPlan);

    VLOG(10) << "got plan: " << physicalPlan->DebugString();

    // set result schema
    auto resultYaml = gopt::GResultSchema::infer(
        *statement->logicalPlan, aliasManager, database->getCatalog());
    return std::make_pair(std::move(*physicalPlan), YAML::Dump(resultYaml));
  } catch (const neug::exception::InvalidArgumentException& e) {
    // return Status(StatusCode::ERR_INVALID_ARGUMENT, e.what());
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT, e.what()));
  } catch (const neug::exception::BinderException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_COMPILATION, e.what()));
  } catch (const neug::exception::CatalogException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_SCHEMA, e.what()));
  } catch (const neug::exception::ConversionException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_TYPE_CONVERSION, e.what()));
  } catch (const neug::exception::InternalException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const neug::exception::NotImplementedException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_NOT_IMPLEMENTED, e.what()));
  } catch (const neug::exception::NotSupportedException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_NOT_SUPPORTED, e.what()));
  } catch (const neug::exception::RuntimeError& e) {
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const neug::exception::TransactionManagerException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const neug::exception::OverflowException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_TYPE_OVERFLOW, e.what()));
  } catch (const neug::exception::PropertyNotFoundException& e) {
    RETURN_ERROR(Status(StatusCode::ERR_PROPERTY_NOT_FOUND, e.what()));
  } catch (const neug::exception::Exception& e) {
    RETURN_ERROR(Status(StatusCode::ERR_COMPILATION, e.what()));
  } catch (const std::exception& e) {
    RETURN_ERROR(Status(StatusCode::ERR_COMPILATION, e.what()));
  } catch (...) {
    RETURN_ERROR(Status(StatusCode::ERR_UNKNOWN,
                        "Unknown error during plan "
                        "compilation"));
  }
}

void GOptPlanner::update_meta(const YAML::Node& schema_yaml_node) {
  VLOG(1) << "[GOptPlanner] update_meta called";
  if (schema_yaml_node.IsNull()) {
    LOG(ERROR) << "Schema YAML node is null";
    return;
  }
  if (!schema_yaml_node.IsMap()) {
    LOG(ERROR) << "Schema YAML node is not a map";
    return;
  }
  database->updateSchema(schema_yaml_node);
}

void GOptPlanner::update_statistics(const std::string& graph_statistic_json) {
  VLOG(1) << "[GOptPlanner] update_statistics called";
  if (graph_statistic_json.empty()) {
    LOG(ERROR) << "Graph statistics JSON is empty";
    return;
  }
  database->updateStats(graph_statistic_json);
}

bool isTokenEnd(char ch) {
  return ch == ' ' || ch == ';' || ch == '\n' || ch == '\t' || ch == '{' ||
         ch == '(';
}

AccessMode GOptPlanner::analyzeMode(const std::string& query) const {
  size_t i = 0;
  const size_t n = query.size();

  while (i < n) {
    while (i < n && isTokenEnd(query[i]))
      ++i;
    if (i >= n)
      break;

    // mark the start pos of current token
    size_t token_start = i;
    bool invalid_token = false;

    // scan the token until a non-alphabetic character or an end character
    while (i < n) {
      char c = query[i];
      if (std::isalpha(static_cast<unsigned char>(c))) {
        ++i;
      } else if (isTokenEnd(c)) {
        break;
      } else {
        invalid_token = true;  // non-alphabetic character found, we need to
                               // skip the current token
        break;
      }
    }

    // if the token is invalid, skip to the next valid token
    if (invalid_token) {
      while (i < n && !isTokenEnd(query[i]))
        ++i;
      ++i;
      continue;
    }

    std::string token(query.data() + token_start, i - token_start);

    if (getSchemaOpTokens().contains(token)) {
      return AccessMode::kSchema;
    }

    if (getUpdateOpTokens().contains(token)) {
      return AccessMode::kUpdate;
    }

    ++i;
  }

  return AccessMode::kRead;
}

const common::case_insensitve_set_t& GOptPlanner::getUpdateOpTokens() const {
  static common::case_insensitve_set_t updateOps = {
      "set", "copy", "checkpoint", "load", "install", "uninstall", "call"};
  return updateOps;
}

const common::case_insensitve_set_t& GOptPlanner::getSchemaOpTokens() const {
  static common::case_insensitve_set_t schemaOps = {"create", "delete", "drop",
                                                    "alter", "rename"};
  return schemaOps;
}
}  // namespace neug