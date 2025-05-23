#pragma once

#include "loaded_extension.h"
#include "main/client_context.h"
#include "main/db_config.h"

namespace kuzu {
namespace extension {

struct ExtensionEntry {
    const char* name;
    const char* extensionName;
};

class ExtensionManager {
public:
    void loadExtension(const std::string& path, main::ClientContext* context);

    KUZU_API std::string toCypher();

    KUZU_API void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue, bool isConfidential);

    const main::ExtensionOption* getExtensionOption(std::string name) const;

    KUZU_API const std::vector<LoadedExtension>& getLoadedExtensions() const {
        return loadedExtensions;
    }

    static std::optional<ExtensionEntry> lookupExtensionsByFunctionName(
        std::string_view functionName);
    static std::optional<ExtensionEntry> lookupExtensionsByTypeName(std::string_view typeName);

    void autoLoadLinkedExtensions(main::ClientContext* context);

private:
    std::vector<LoadedExtension> loadedExtensions;
    std::unordered_map<std::string, main::ExtensionOption> extensionOptions;
};

} // namespace extension
} // namespace kuzu
