/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/compiler/gopt/g_vfs_holder.h"

namespace neug {
namespace common {

VirtualFileSystem* VFSHolder::vfs = nullptr;

void VFSHolder::setVFS(VirtualFileSystem* fileSystem) {
  if (!fileSystem) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Cannot set VirtualFileSystem to holder, it is nullptr");
  }
  vfs = fileSystem;
}

VirtualFileSystem* VFSHolder::getVFS() {
  if (!vfs) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Cannot get VirtualFileSystem from holder, it is not set");
  }
  return vfs;
}

}  // namespace common
}  // namespace neug