/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "neug/compiler/gopt/g_catalog_holder.h"

namespace neug {
namespace catalog {
GCatalog* GCatalogHolder::gCatalog = nullptr;

void GCatalogHolder::setGCatalog(GCatalog* catalog) {
  if (!catalog) {
    THROW_CATALOG_EXCEPTION("cannot set GCatalog to holder, it is nullptr");
  }
  gCatalog = catalog;
}

GCatalog* GCatalogHolder::getGCatalog() {
  if (!gCatalog) {
    THROW_CATALOG_EXCEPTION("cannot get GCatalog from holder, it is not set");
  }
  return gCatalog;
}

}  // namespace catalog
}  // namespace neug