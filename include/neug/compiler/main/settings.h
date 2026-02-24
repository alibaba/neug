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

#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/option_config.h"

namespace neug {
namespace main {

struct ThreadsSetting {
  static constexpr auto name = "threads";
  static constexpr auto inputType = common::LogicalTypeID::UINT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->numThreads =
        parameter.getValue<uint64_t>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->numThreads);
  }
};

struct WarningLimitSetting {
  static constexpr auto name = "warning_limit";
  static constexpr auto inputType = common::LogicalTypeID::UINT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->warningLimit =
        parameter.getValue<uint64_t>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->warningLimit);
  }
};

struct TimeoutSetting {
  static constexpr auto name = "timeout";
  static constexpr auto inputType = common::LogicalTypeID::UINT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->timeoutInMS =
        parameter.getValue<uint64_t>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->timeoutInMS);
  }
};

struct ProgressBarSetting {
  static constexpr auto name = "progress_bar";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {}
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->enableProgressBar);
  }
};

struct VarLengthExtendMaxDepthSetting {
  static constexpr auto name = "var_length_extend_max_depth";
  static constexpr auto inputType = common::LogicalTypeID::INT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->varLengthMaxDepth =
        parameter.getValue<int64_t>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->varLengthMaxDepth);
  }
};

struct SparseFrontierThresholdSetting {
  static constexpr auto name = "sparse_frontier_threshold";
  static constexpr auto inputType = common::LogicalTypeID::INT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->sparseFrontierThreshold =
        parameter.getValue<int64_t>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->sparseFrontierThreshold);
  }
};

struct EnableSemiMaskSetting {
  static constexpr auto name = "enable_semi_mask";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->enableSemiMask =
        parameter.getValue<bool>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->enableSemiMask);
  }
};

struct DisableMapKeyCheck {
  static constexpr auto name = "disable_map_key_check";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->disableMapKeyCheck =
        parameter.getValue<bool>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->disableMapKeyCheck);
  }
};

struct EnableZoneMapSetting {
  static constexpr auto name = "enable_zone_map";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->enableZoneMap =
        parameter.getValue<bool>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(context->getClientConfig()->enableZoneMap);
  }
};

struct HomeDirectorySetting {
  static constexpr auto name = "home_directory";
  static constexpr auto inputType = common::LogicalTypeID::STRING;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->homeDirectory =
        parameter.getValue<std::string>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value::createValue(
        context->getClientConfig()->homeDirectory);
  }
};

struct FileSearchPathSetting {
  static constexpr auto name = "file_search_path";
  static constexpr auto inputType = common::LogicalTypeID::STRING;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->fileSearchPath =
        parameter.getValue<std::string>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value::createValue(
        context->getClientConfig()->fileSearchPath);
  }
};

struct RecursivePatternSemanticSetting {
  static constexpr auto name = "recursive_pattern_semantic";
  static constexpr auto inputType = common::LogicalTypeID::STRING;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    const auto input = parameter.getValue<std::string>();
    context->getClientConfigUnsafe()->recursivePatternSemantic =
        common::PathSemanticUtils::fromString(input);
  }
  static common::Value getSetting(const ClientContext* context) {
    const auto result = common::PathSemanticUtils::toString(
        context->getClientConfig()->recursivePatternSemantic);
    return common::Value::createValue(result);
  }
};

struct RecursivePatternFactorSetting {
  static constexpr auto name = "recursive_pattern_factor";
  static constexpr auto inputType = common::LogicalTypeID::INT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->recursivePatternCardinalityScaleFactor =
        parameter.getValue<std::int64_t>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value::createValue(
        context->getClientConfig()->recursivePatternCardinalityScaleFactor);
  }
};

struct EnableMVCCSetting {
  static constexpr auto name = "debug_enable_multi_writes";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {}
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(false);
  }
};

struct CheckpointThresholdSetting {
  static constexpr auto name = "checkpoint_threshold";
  static constexpr auto inputType = common::LogicalTypeID::INT64;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {}
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(0);
  }
};

struct AutoCheckpointSetting {
  static constexpr auto name = "auto_checkpoint";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {}
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(false);
  }
};

struct ForceCheckpointClosingDBSetting {
  static constexpr auto name = "force_checkpoint_on_close";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {}
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(false);
  }
};

struct SpillToDiskSetting {
  static constexpr auto name = "spill_to_disk";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter);
  static common::Value getSetting(const ClientContext* context) {
    return common::Value(false);
  }
};

struct EnableOptimizerSetting {
  static constexpr auto name = "enable_plan_optimizer";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->enablePlanOptimizer =
        parameter.getValue<bool>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value::createValue(
        context->getClientConfig()->enablePlanOptimizer);
  }
};

struct EnableInternalCatalogSetting {
  static constexpr auto name = "enable_internal_catalog";
  static constexpr auto inputType = common::LogicalTypeID::BOOL;
  static void setContext(ClientContext* context,
                         const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getClientConfigUnsafe()->enableInternalCatalog =
        parameter.getValue<bool>();
  }
  static common::Value getSetting(const ClientContext* context) {
    return common::Value::createValue(
        context->getClientConfig()->enableInternalCatalog);
  }
};

}  // namespace main
}  // namespace neug
