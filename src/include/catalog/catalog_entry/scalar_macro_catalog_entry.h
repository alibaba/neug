#pragma once

#include "src/include/catalog/catalog_entry/catalog_entry.h"
#include "src/include/function/scalar_macro_function.h"

namespace gs {
namespace catalog {

class ScalarMacroCatalogEntry final : public CatalogEntry {
 public:
  //===--------------------------------------------------------------------===//
  // constructors
  //===--------------------------------------------------------------------===//
  ScalarMacroCatalogEntry() = default;
  ScalarMacroCatalogEntry(
      std::string name,
      std::unique_ptr<function::ScalarMacroFunction> macroFunction);

  //===--------------------------------------------------------------------===//
  // getter & setter
  //===--------------------------------------------------------------------===//
  function::ScalarMacroFunction* getMacroFunction() const {
    return macroFunction.get();
  }

  //===--------------------------------------------------------------------===//
  // serialization & deserialization
  //===--------------------------------------------------------------------===//
  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<ScalarMacroCatalogEntry> deserialize(
      common::Deserializer& deserializer);
  std::string toCypher(const ToCypherInfo& info) const override;

 private:
  std::unique_ptr<function::ScalarMacroFunction> macroFunction;
};

}  // namespace catalog
}  // namespace gs
