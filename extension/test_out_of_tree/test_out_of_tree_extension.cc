#include "neug/compiler/extension/extension_api.h"

extern "C" {

void Init() {
  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          "test_out_of_tree",
          "Test extension for validating out-of-tree extension builds."});
}

const char* Name() { return "TEST_OUT_OF_TREE"; }

}  // extern "C"
