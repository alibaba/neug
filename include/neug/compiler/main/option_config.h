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

#include <string>

#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace common {
class Value;
enum class LogicalTypeID : uint8_t;
}  // namespace common

namespace main {

class ClientContext;
struct SystemConfig;

typedef void (*set_context)(ClientContext* context,
                            const common::Value& parameter);
typedef common::Value (*get_setting)(const ClientContext* context);

enum class OptionType : uint8_t { CONFIGURATION = 0, EXTENSION = 1 };

struct Option {
  std::string name;
  common::LogicalTypeID parameterType;
  OptionType optionType;
  bool isConfidential;

  Option(std::string name, common::LogicalTypeID parameterType,
         OptionType optionType, bool isConfidential)
      : name{std::move(name)},
        parameterType{parameterType},
        optionType{optionType},
        isConfidential{isConfidential} {}

  virtual ~Option() = default;
};

struct ConfigurationOption final : Option {
  set_context setContext;
  get_setting getSetting;

  ConfigurationOption(std::string name, common::LogicalTypeID parameterType,
                      set_context setContext, get_setting getSetting)
      : Option{std::move(name), parameterType, OptionType::CONFIGURATION,
               false /* isConfidential */},
        setContext{setContext},
        getSetting{getSetting} {}
};

struct ExtensionOption final : Option {
  common::Value defaultValue;

  ExtensionOption(std::string name, common::LogicalTypeID parameterType,
                  common::Value defaultValue, bool isConfidential)
      : Option{std::move(name), parameterType, OptionType::EXTENSION,
               isConfidential},
        defaultValue{std::move(defaultValue)} {}
};

}  // namespace main
}  // namespace neug
