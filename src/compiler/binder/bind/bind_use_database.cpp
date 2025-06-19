#include "src/include/binder/binder.h"
#include "src/include/binder/bound_use_database.h"
#include "src/include/parser/use_database.h"

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindUseDatabase(
    const parser::Statement& statement) {
  auto useDatabase = statement.constCast<parser::UseDatabase>();
  return std::make_unique<BoundUseDatabase>(useDatabase.getDBName());
}

}  // namespace binder
}  // namespace gs
