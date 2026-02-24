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

#include "neug/utils/bitset.h"

#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

void Bitset::Serialize(std::ostream& os) const {
  InArchive arc;
  arc << size_ << size_in_words_ << capacity_ << capacity_in_words_;
  size_t size = arc.GetSize();
  os.write(reinterpret_cast<const char*>(&size), sizeof(size));
  os.write(arc.GetBuffer(), arc.GetSize());
  if (size_in_words_ > 0) {
    os.write(reinterpret_cast<const char*>(data_),
             size_in_words_ * sizeof(uint64_t));
  }
}

void Bitset::Deserialize(std::istream& is) {
  OutArchive arc;
  size_t size;
  is.read(reinterpret_cast<char*>(&size), sizeof(size));
  arc.Allocate(size);
  is.read(arc.GetBuffer(), size);
  arc >> size_ >> size_in_words_ >> capacity_ >> capacity_in_words_;
  if (data_ != nullptr) {
    free(data_);
  }
  data_ = static_cast<uint64_t*>(malloc(capacity_in_words_ * sizeof(uint64_t)));
  if (size_in_words_ > 0) {
    is.read(reinterpret_cast<char*>(data_), size_in_words_ * sizeof(uint64_t));
  }
}

}  // namespace neug
