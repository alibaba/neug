#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_create_macro.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/create_macro.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::parser;

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCreateMacro(
    const Statement& statement) const {
  auto& createMacro = ku_dynamic_cast<const CreateMacro&>(statement);
  auto macroName = createMacro.getMacroName();
  StringUtils::toUpper(macroName);
  if (clientContext->getCatalog()->containsMacro(
          clientContext->getTransaction(), macroName)) {
    throw BinderException{stringFormat("Macro {} already exists.", macroName)};
  }
  parser::default_macro_args defaultArgs;
  for (auto& defaultArg : createMacro.getDefaultArgs()) {
    defaultArgs.emplace_back(defaultArg.first, defaultArg.second->copy());
  }
  auto scalarMacro = std::make_unique<function::ScalarMacroFunction>(
      createMacro.getMacroExpression()->copy(), createMacro.getPositionalArgs(),
      std::move(defaultArgs));
  return std::make_unique<BoundCreateMacro>(std::move(macroName),
                                            std::move(scalarMacro));
}

}  // namespace binder
}  // namespace gs
