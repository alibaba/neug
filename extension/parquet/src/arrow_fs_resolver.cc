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

#include "parquet/arrow_fs_resolver.h"

#include <arrow/filesystem/localfs.h>
#include <memory>

#include "neug/utils/file_sys/file_system.h"

namespace neug {
namespace parquet {

std::shared_ptr<arrow::fs::FileSystem> resolveArrowFileSystem(
    const fsys::FileSystem& fs) {
  if (auto opaque = fs.getArrowFileSystem()) {
    return std::static_pointer_cast<arrow::fs::FileSystem>(opaque);
  }
  return std::make_shared<arrow::fs::LocalFileSystem>();
}

}  // namespace parquet
}  // namespace neug
