#include "src/include/binder/binder.h"
#include "src/include/binder/bound_create_macro.h"
#include "src/include/catalog/catalog.h"
#include "src/include/common/exception/binder.h"
#include "src/include/common/string_format.h"
#include "src/include/common/string_utils.h"
#include "src/include/main/client_context.h"
#include "src/include/parser/create_macro.h"

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
