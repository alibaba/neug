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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
#pragma once

#include "neug/utils/io/stream/input_stream.h"

namespace neug {

namespace fsys {
class FileSystem;
}  // namespace fsys

namespace function {

/// Builds a stream opener over the remote backend of the given file
/// system (http/https/s3/oss, resolved through the VFS registry).
/// Returns a null function when the file system has no remote backend
/// (local paths); callers then keep plain local file IO.
io::InputStreamOpener makeImportStreamOpener(const fsys::FileSystem& fs);

}  // namespace function
}  // namespace neug
