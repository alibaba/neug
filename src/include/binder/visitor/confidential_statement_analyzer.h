#pragma once

#include "src/include/binder/bound_statement_visitor.h"

namespace gs {
namespace binder {

class ConfidentialStatementAnalyzer final : public BoundStatementVisitor {
 public:
  bool isConfidential() const { return confidentialStatement; }

 private:
  void visitStandaloneCall(const BoundStatement& boundStatement) override;

 private:
  bool confidentialStatement = false;
};

}  // namespace binder
}  // namespace gs
