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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/parser/query/graph_pattern/pattern_element.h"
#include "neug/compiler/parser/transformer.h"

using namespace neug::common;

namespace neug {
namespace parser {

std::vector<PatternElement> Transformer::transformPattern(
    CypherParser::OC_PatternContext& ctx) {
  std::vector<PatternElement> patterns;
  for (auto& patternPart : ctx.oC_PatternPart()) {
    patterns.push_back(transformPatternPart(*patternPart));
  }
  return patterns;
}

PatternElement Transformer::transformPatternPart(
    CypherParser::OC_PatternPartContext& ctx) {
  auto patternElement =
      transformAnonymousPatternPart(*ctx.oC_AnonymousPatternPart());
  if (ctx.oC_Variable()) {
    auto variable = transformVariable(*ctx.oC_Variable());
    patternElement.setPathName(variable);
  }
  return patternElement;
}

PatternElement Transformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext& ctx) {
  return transformPatternElement(*ctx.oC_PatternElement());
}

PatternElement Transformer::transformPatternElement(
    CypherParser::OC_PatternElementContext& ctx) {
  if (ctx.oC_PatternElement()) {  // parenthesized pattern element
    return transformPatternElement(*ctx.oC_PatternElement());
  }
  auto patternElement =
      PatternElement(transformNodePattern(*ctx.oC_NodePattern()));
  if (!ctx.oC_PatternElementChain().empty()) {
    for (auto& patternElementChain : ctx.oC_PatternElementChain()) {
      patternElement.addPatternElementChain(
          transformPatternElementChain(*patternElementChain));
    }
  }
  return patternElement;
}

NodePattern Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext& ctx) {
  auto variable = std::string();
  if (ctx.oC_Variable()) {
    variable = transformVariable(*ctx.oC_Variable());
  }
  auto nodeLabels = std::vector<std::string>{};
  if (ctx.oC_NodeLabels()) {
    nodeLabels = transformNodeLabels(*ctx.oC_NodeLabels());
  }
  auto properties =
      std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>{};
  if (ctx.nEUG_Properties()) {
    properties = transformProperties(*ctx.nEUG_Properties());
  }
  return NodePattern(std::move(variable), std::move(nodeLabels),
                     std::move(properties));
}

PatternElementChain Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext& ctx) {
  return PatternElementChain(
      transformRelationshipPattern(*ctx.oC_RelationshipPattern()),
      transformNodePattern(*ctx.oC_NodePattern()));
}

RelPattern Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext& ctx) {
  auto relDetail = ctx.oC_RelationshipDetail();
  auto variable = std::string();
  auto relTypes = std::vector<std::string>{};
  auto properties =
      std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>{};
  // Parse name, label & properties
  if (relDetail) {
    if (relDetail->oC_Variable()) {
      variable = transformVariable(*relDetail->oC_Variable());
    }
    if (relDetail->oC_RelationshipTypes()) {
      relTypes = transformRelTypes(*relDetail->oC_RelationshipTypes());
    }
    if (relDetail->nEUG_Properties()) {
      properties = transformProperties(*relDetail->nEUG_Properties());
    }
  }
  // Parse direction
  ArrowDirection arrowDirection;  // NOLINT(*-init-variables)
  if (ctx.oC_LeftArrowHead()) {
    arrowDirection = ArrowDirection::LEFT;
  } else if (ctx.oC_RightArrowHead()) {
    arrowDirection = ArrowDirection::RIGHT;
  } else {
    arrowDirection = ArrowDirection::BOTH;
  }
  // Parse recursive info
  auto relType = QueryRelType::NON_RECURSIVE;
  auto recursiveInfo = RecursiveRelPatternInfo();

  if (relDetail && relDetail->nEUG_RecursiveDetail()) {
    auto recursiveDetail = relDetail->nEUG_RecursiveDetail();
    // Parse recursive type
    auto recursiveType = recursiveDetail->nEUG_RecursiveType();
    if (recursiveType) {
      if (recursiveType->ALL()) {
        if (recursiveType->WSHORTEST()) {
          relType = QueryRelType::ALL_WEIGHTED_SHORTEST;
          recursiveInfo.weightPropertyName =
              transformPropertyKeyName(*recursiveType->oC_PropertyKeyName());
        } else {
          relType = QueryRelType::ALL_SHORTEST;
        }
      } else if (recursiveType->WSHORTEST()) {
        relType = QueryRelType::WEIGHTED_SHORTEST;
        recursiveInfo.weightPropertyName =
            transformPropertyKeyName(*recursiveType->oC_PropertyKeyName());
      } else if (recursiveDetail->nEUG_RecursiveType()->SHORTEST()) {
        relType = QueryRelType::SHORTEST;
      } else if (recursiveDetail->nEUG_RecursiveType()->TRAIL()) {
        relType = QueryRelType::VARIABLE_LENGTH_TRAIL;
      } else if (recursiveDetail->nEUG_RecursiveType()->ACYCLIC()) {
        relType = QueryRelType::VARIABLE_LENGTH_ACYCLIC;
      } else {
        relType = QueryRelType::VARIABLE_LENGTH_WALK;
      }
    } else {
      relType = QueryRelType::VARIABLE_LENGTH_WALK;
    }
    // Parse lower, upper bound
    auto lowerBound = std::string("1");
    auto upperBound = std::string("");
    auto range = recursiveDetail->oC_RangeLiteral();
    if (range) {
      if (range->oC_IntegerLiteral()) {
        lowerBound = range->oC_IntegerLiteral()->getText();
        upperBound = lowerBound;
      }
      if (range->oC_LowerBound()) {
        lowerBound = range->oC_LowerBound()->getText();
      }
      if (range->oC_UpperBound()) {
        upperBound = range->oC_UpperBound()->getText();
      }
    }
    recursiveInfo.lowerBound = lowerBound;
    recursiveInfo.upperBound = upperBound;
    // Parse recursive comprehension
    auto comprehension = recursiveDetail->nEUG_RecursiveComprehension();
    if (comprehension) {
      recursiveInfo.relName = transformVariable(*comprehension->oC_Variable(0));
      recursiveInfo.nodeName =
          transformVariable(*comprehension->oC_Variable(1));
      if (comprehension->oC_Where()) {
        recursiveInfo.whereExpression =
            transformWhere(*comprehension->oC_Where());
      }
      if (!comprehension->nEUG_RecursiveProjectionItems().empty()) {
        recursiveInfo.hasProjection = true;
        NEUG_ASSERT(comprehension->nEUG_RecursiveProjectionItems().size() == 2);
        auto relProjectionList = comprehension->nEUG_RecursiveProjectionItems(0)
                                     ->oC_ProjectionItems();
        if (relProjectionList) {
          recursiveInfo.relProjectionList =
              transformProjectionItems(*relProjectionList);
        }
        auto nodeProjectionList =
            comprehension->nEUG_RecursiveProjectionItems(1)
                ->oC_ProjectionItems();
        if (nodeProjectionList) {
          recursiveInfo.nodeProjectionList =
              transformProjectionItems(*nodeProjectionList);
        }
      }
    }
  }
  return RelPattern(variable, relTypes, relType, arrowDirection,
                    std::move(properties), std::move(recursiveInfo));
}

std::vector<s_parsed_expr_pair> Transformer::transformProperties(
    CypherParser::NEUG_PropertiesContext& ctx) {
  std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> result;
  NEUG_ASSERT(ctx.oC_PropertyKeyName().size() == ctx.oC_Expression().size());
  for (auto i = 0u; i < ctx.oC_PropertyKeyName().size(); ++i) {
    auto propertyKeyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName(i));
    auto expression = transformExpression(*ctx.oC_Expression(i));
    result.emplace_back(propertyKeyName, std::move(expression));
  }
  return result;
}

std::vector<std::string> Transformer::transformRelTypes(
    CypherParser::OC_RelationshipTypesContext& ctx) {
  std::vector<std::string> relTypes;
  for (auto& relType : ctx.oC_RelTypeName()) {
    relTypes.push_back(transformRelTypeName(*relType));
  }
  return relTypes;
}

std::vector<std::string> Transformer::transformNodeLabels(
    CypherParser::OC_NodeLabelsContext& ctx) {
  std::vector<std::string> nodeLabels;
  for (auto& nodeLabel : ctx.oC_NodeLabel()) {
    nodeLabels.push_back(transformNodeLabel(*nodeLabel));
  }
  return nodeLabels;
}

std::string Transformer::transformNodeLabel(
    CypherParser::OC_NodeLabelContext& ctx) {
  return transformLabelName(*ctx.oC_LabelName());
}

std::string Transformer::transformLabelName(
    CypherParser::OC_LabelNameContext& ctx) {
  return transformSchemaName(*ctx.oC_SchemaName());
}

std::string Transformer::transformRelTypeName(
    CypherParser::OC_RelTypeNameContext& ctx) {
  return transformSchemaName(*ctx.oC_SchemaName());
}

}  // namespace parser
}  // namespace neug
