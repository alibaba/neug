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

#pragma once

#include "neug/compiler/gopt/g_type_registry.h"

#define REGISTER_YAML_NODE_TYPE(YAML_NODE_EXPR, TYPE_ID)  \
  namespace {                                             \
  struct _RegisterYaml_##TYPE_ID {                        \
    _RegisterYaml_##TYPE_ID() {                           \
      YAML::Node node = YAML_NODE_EXPR;                   \
      neug::common::LogicalTypeRegistry::registerType(    \
          node, neug::common::LogicalTypeID::TYPE_ID);    \
    }                                                     \
  };                                                      \
  static _RegisterYaml_##TYPE_ID _registerYaml_##TYPE_ID; \
  }

#define YAML_NODE_DT_SIGNED_INT64            \
  [] {                                       \
    YAML::Node n;                            \
    n["primitive_type"] = "DT_SIGNED_INT64"; \
    return n;                                \
  }()

#define YAML_NODE_DT_UNSIGNED_INT64            \
  [] {                                         \
    YAML::Node n;                              \
    n["primitive_type"] = "DT_UNSIGNED_INT64"; \
    return n;                                  \
  }()

#define YAML_NODE_DT_SIGNED_INT32            \
  [] {                                       \
    YAML::Node n;                            \
    n["primitive_type"] = "DT_SIGNED_INT32"; \
    return n;                                \
  }()

#define YAML_NODE_DT_UNSIGNED_INT32            \
  [] {                                         \
    YAML::Node n;                              \
    n["primitive_type"] = "DT_UNSIGNED_INT32"; \
    return n;                                  \
  }()

#define YAML_NODE_DT_FLOAT            \
  [] {                                \
    YAML::Node n;                     \
    n["primitive_type"] = "DT_FLOAT"; \
    return n;                         \
  }()

#define YAML_NODE_DT_DOUBLE            \
  [] {                                 \
    YAML::Node n;                      \
    n["primitive_type"] = "DT_DOUBLE"; \
    return n;                          \
  }()

#define YAML_NODE_DT_BOOL            \
  [] {                               \
    YAML::Node n;                    \
    n["primitive_type"] = "DT_BOOL"; \
    return n;                        \
  }()

#define YAML_NODE_DT_ANY            \
  [] {                              \
    YAML::Node n;                   \
    n["primitive_type"] = "DT_ANY"; \
    return n;                       \
  }()

#define YAML_NODE_STRING_LONG_TEXT \
  [] {                             \
    YAML::Node n;                  \
    n["string"]["long_text"] = ""; \
    return n;                      \
  }()

#define YAML_NODE_STRING_VARCHAR(max_length)            \
  [] {                                                  \
    YAML::Node n;                                       \
    n["string"]["var_char"]["max_length"] = max_length; \
    return n;                                           \
  }()

#define YAML_NODE_TEMPORAL_DATE()                           \
  [] {                                                      \
    YAML::Node n;                                           \
    n["temporal"]["date"]["date_format"] = "DF_YYYY_MM_DD"; \
    return n;                                               \
  }()

#define YAML_NODE_TEMPORAL_DATETIME()                          \
  [] {                                                         \
    YAML::Node n;                                              \
    n["temporal"]["datetime"]["date_time_format"] =            \
        "DTF_YYYY_MM_DD_HH_MM_SS_SSS";                         \
    n["temporal"]["datetime"]["time_zone_format"] = "TZF_UTC"; \
    return n;                                                  \
  }()

#define YAML_NODE_TEMPORAL_INTERVAL() \
  [] {                                \
    YAML::Node n;                     \
    n["temporal"]["interval"] = "";   \
    return n;                         \
  }()

#define YAML_NODE_TEMPORAL_DATE32() \
  [] {                              \
    YAML::Node n;                   \
    n["temporal"]["date32"] = "";   \
    return n;                       \
  }()

#define YAML_NODE_TEMPORAL_TIMESTAMP64() \
  [] {                                   \
    YAML::Node n;                        \
    n["temporal"]["timestamp"] = "";     \
    return n;                            \
  }()

// Type registrations are in src/compiler/gopt/g_type_registration.cpp
// to avoid static-init-order issues when extensions are dlopen'd.
