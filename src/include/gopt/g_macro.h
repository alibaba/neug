#pragma once

#include "src/include/gopt/g_type_registry.h"

#define REGISTER_YAML_NODE_TYPE(YAML_NODE_EXPR, TYPE_ID)  \
  namespace {                                             \
  struct _RegisterYaml_##TYPE_ID {                        \
    _RegisterYaml_##TYPE_ID() {                           \
      YAML::Node node = YAML_NODE_EXPR;                   \
      gs::common::LogicalTypeRegistry::registerType(      \
          node, gs::common::LogicalTypeID::TYPE_ID);      \
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

REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_SIGNED_INT64, INT64)
REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_UNSIGNED_INT64, UINT64)
REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_SIGNED_INT32, INT32)
REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_UNSIGNED_INT32, UINT32)
REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_FLOAT, FLOAT)
REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_DOUBLE, DOUBLE)
REGISTER_YAML_NODE_TYPE(YAML_NODE_DT_BOOL, BOOL)
REGISTER_YAML_NODE_TYPE(YAML_NODE_STRING_LONG_TEXT, STRING)
