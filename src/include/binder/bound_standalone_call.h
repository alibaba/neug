#pragma once

#include "src/include/binder/bound_statement.h"
#include "src/include/binder/expression/expression.h"
#include "src/include/main/db_config.h"

namespace gs {
namespace binder {

class BoundStandaloneCall final : public BoundStatement {
 public:
  BoundStandaloneCall(const main::Option* option,
                      std::shared_ptr<Expression> optionValue)
      : BoundStatement{common::StatementType::STANDALONE_CALL,
                       BoundStatementResult::createEmptyResult()},
        option{option},
        optionValue{std::move(optionValue)} {}

  const main::Option* getOption() const { return option; }

  std::shared_ptr<Expression> getOptionValue() const { return optionValue; }

 private:
  const main::Option* option;
  std::shared_ptr<Expression> optionValue;
};

}  // namespace binder
}  // namespace gs
