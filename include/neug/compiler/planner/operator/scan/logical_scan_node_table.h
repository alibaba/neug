#pragma once

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/storage/predicate/column_predicate.h"

namespace neug {
namespace planner {

enum class LogicalScanNodeTableType : uint8_t {
  SCAN = 0,
  PRIMARY_KEY_SCAN = 1,
};

struct ExtraScanNodeTableInfo {
  virtual ~ExtraScanNodeTableInfo() = default;
  virtual std::unique_ptr<ExtraScanNodeTableInfo> copy() const = 0;

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
};

struct PrimaryKeyScanInfo final : ExtraScanNodeTableInfo {
  std::shared_ptr<binder::Expression> key;

  explicit PrimaryKeyScanInfo(std::shared_ptr<binder::Expression> key)
      : key{std::move(key)} {}

  std::unique_ptr<ExtraScanNodeTableInfo> copy() const override {
    return std::make_unique<PrimaryKeyScanInfo>(key);
  }
};

struct PrimaryKey {
  std::string key;
  PrimaryKeyScanInfo* value;

  PrimaryKey(const std::string& key, PrimaryKeyScanInfo* value)
      : key(key), value(value) {}
};

struct LogicalScanNodeTablePrintInfo final : OPPrintInfo {
  std::shared_ptr<binder::Expression> nodeID;
  binder::expression_vector properties;

  LogicalScanNodeTablePrintInfo(std::shared_ptr<binder::Expression> nodeID,
                                binder::expression_vector properties)
      : nodeID{std::move(nodeID)}, properties{std::move(properties)} {}

  std::string toString() const override {
    auto result = "Tables: " + nodeID->toString();
    if (nodeID->hasAlias()) {
      result += "Alias: " + nodeID->getAlias();
    }
    result += ",Properties :" + binder::ExpressionUtil::toString(properties);
    return result;
  }
};

class LogicalScanNodeTable final : public LogicalOperator {
  static constexpr LogicalOperatorType type_ =
      LogicalOperatorType::SCAN_NODE_TABLE;
  static constexpr LogicalScanNodeTableType defaultScanType =
      LogicalScanNodeTableType::SCAN;

 public:
  LogicalScanNodeTable(std::shared_ptr<binder::Expression> nodeID,
                       std::vector<common::table_id_t> nodeTableIDs,
                       binder::expression_vector properties,
                       common::cardinality_t cardinality = 0)
      : LogicalOperator{type_},
        scanType{defaultScanType},
        nodeID{std::move(nodeID)},
        nodeTableIDs{std::move(nodeTableIDs)},
        properties{std::move(properties)} {
    this->cardinality = cardinality;
  }
  LogicalScanNodeTable(const LogicalScanNodeTable& other);

  void computeFactorizedSchema() override;
  void computeFlatSchema() override;

  std::string getAliasName() const;

  gopt::GAliasName getGAliasName() const;

  std::optional<PrimaryKey> getPrimaryKey(catalog::Catalog* catalog) const;

  std::unique_ptr<gopt::GNodeType> getNodeType(catalog::Catalog* catalog) const;

  std::string getExpressionsForPrinting() const override {
    auto message =
        nodeID->toString() + " " + binder::ExpressionUtil::toString(properties);
    auto extraInfo = getExtraInfo();
    if (extraInfo != nullptr) {
      auto pkExtraInfo = dynamic_cast<PrimaryKeyScanInfo*>(extraInfo);
      if (pkExtraInfo != nullptr) {
        message += " PK_SCAN(" + pkExtraInfo->key->toString() + ")";
      }
    }
    message += ("Type: " + nodeID->getDataType().toString());
    if (predicates != nullptr) {
      message += " Predicates: " + predicates->toString();
    }
    message += " Cardinality: " + std::to_string(cardinality);
    if (isNodeIDScan()) {
      message += " NodeIDScan";
    }
    return message;
  }

  LogicalScanNodeTableType getScanType() const { return scanType; }
  void setScanType(LogicalScanNodeTableType scanType_) { scanType = scanType_; }

  std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
  std::vector<common::table_id_t> getTableIDs() const { return nodeTableIDs; }

  void setTableIDs(std::vector<common::table_id_t> tableIDs) {
    nodeTableIDs = std::move(tableIDs);
  }

  binder::expression_vector getProperties() const { return properties; }
  void addProperty(std::shared_ptr<binder::Expression> expr) {
    properties.push_back(std::move(expr));
  }
  void setPropertyPredicates(
      std::vector<storage::ColumnPredicateSet> predicates) {
    propertyPredicates = std::move(predicates);
  }
  const std::vector<storage::ColumnPredicateSet>& getPropertyPredicates()
      const {
    return propertyPredicates;
  }

  void setExtraInfo(std::unique_ptr<ExtraScanNodeTableInfo> info) {
    extraInfo = std::move(info);
  }

  ExtraScanNodeTableInfo* getExtraInfo() const { return extraInfo.get(); }

  std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
    return std::make_unique<LogicalScanNodeTablePrintInfo>(nodeID, properties);
  }

  std::unique_ptr<LogicalOperator> copy() override;

  void setPredicates(std::shared_ptr<binder::Expression> predicates_) {
    predicates = std::move(predicates_);
  }

  std::shared_ptr<binder::Expression> getPredicates() const {
    return predicates;
  }

  void setNodeIDScan(bool nodeIDScan_) { nodeIDScan = nodeIDScan_; }
  bool isNodeIDScan() const { return nodeIDScan; }

 private:
  LogicalScanNodeTableType scanType;
  std::shared_ptr<binder::Expression> nodeID;
  std::vector<common::table_id_t> nodeTableIDs;
  binder::expression_vector properties;
  std::vector<storage::ColumnPredicateSet> propertyPredicates;
  std::unique_ptr<ExtraScanNodeTableInfo> extraInfo;
  std::shared_ptr<binder::Expression> predicates;
  bool nodeIDScan = false;
};

}  // namespace planner
}  // namespace neug
