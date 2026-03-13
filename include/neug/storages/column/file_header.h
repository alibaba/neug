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

#include <openssl/md5.h>

namespace neug {

/**
 * @brief File header structure containing MD5 checksum for data integrity.
 *
 * This header is prepended to data files to enable dirty checking
 * and data integrity verification.
 */
struct FileHeader {
  unsigned char data_md5[MD5_DIGEST_LENGTH];
};

}  // namespace neug