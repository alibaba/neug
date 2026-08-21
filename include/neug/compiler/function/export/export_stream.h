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

#include <memory>

#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/stream/output_stream.h"

namespace neug {
namespace function {

/// Opens an output stream for an export target path. Remote schemes
/// (http/https/s3/oss, resolved through the VFS registry) are written via
/// the registered RemoteFileSystem; every other scheme falls back to a
/// local file.
std::unique_ptr<io::OutputStream> openExportOutputStream(
    const reader::FileSchema& schema);

}  // namespace function
}  // namespace neug
