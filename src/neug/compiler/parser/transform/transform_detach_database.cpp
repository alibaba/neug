#include "neug/compiler/parser/detach_database.h"
#include "neug/compiler/parser/transformer.h"

namespace gs {
namespace parser {

std::unique_ptr<Statement> Transformer::transformDetachDatabase(
    CypherParser::KU_DetachDatabaseContext& ctx) {
  auto dbName = transformSchemaName(*ctx.oC_SchemaName());
  return std::make_unique<DetachDatabase>(std::move(dbName));
}

}  // namespace parser
}  // namespace gs
