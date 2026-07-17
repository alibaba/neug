#include "neug/compiler/extension/extension_api.h"

extern "C" {

void Init() {
  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          "ci_out_of_tree",
          "Test extension for validating out-of-tree extension builds."});
}

const char* Name() { return "CI_OUT_OF_TREE"; }

}  // extern "C"
