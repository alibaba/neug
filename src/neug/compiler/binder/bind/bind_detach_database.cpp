#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_detach_database.h"
#include "neug/compiler/parser/detach_database.h"

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindDetachDatabase(
    const parser::Statement& statement) {
  auto& detachDatabase = statement.constCast<parser::DetachDatabase>();
  return std::make_unique<BoundDetachDatabase>(detachDatabase.getDBName());
}

}  // namespace binder
}  // namespace gs
