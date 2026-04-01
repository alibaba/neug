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

#include <string>

#include "neug/utils/property/property.h"

namespace neug {
namespace execution {

// OwnedProperty wraps a Property together with optional owned memory that
// backs the Property's string_view (used for List, VARCHAR or other non-pod
// types). This is needed because Property is a pure-view type that never owns
// memory, but Value->Property conversion for List types produces a temporary
// blob that must outlive the Property until the storage layer copies the data.
struct OwnedProperty {
  OwnedProperty() = default;

  explicit OwnedProperty(Property p) : prop_(std::move(p)) {}
  OwnedProperty(Property p, std::string owned)
      : owned_buffer_(std::move(owned)), prop_(std::move(p)) {
    reseat();
  }
  OwnedProperty(OwnedProperty&& other) noexcept
      : owned_buffer_(std::move(other.owned_buffer_)), prop_(other.prop_) {
    reseat();
  }

  OwnedProperty& operator=(OwnedProperty&& other) noexcept {
    if (this != &other) {
      owned_buffer_ = std::move(other.owned_buffer_);
      prop_ = other.prop_;
      reseat();
    }
    return *this;
  }

  // Copy constructor: deep-copy the buffer and re-seat
  OwnedProperty(const OwnedProperty& other)
      : owned_buffer_(other.owned_buffer_), prop_(other.prop_) {
    reseat();
  }

  OwnedProperty& operator=(const OwnedProperty& other) {
    if (this != &other) {
      owned_buffer_ = other.owned_buffer_;
      prop_ = other.prop_;
      reseat();
    }
    return *this;
  }

  // Access the underlying Property (for passing to storage layer APIs).
  const Property& prop() const { return prop_; }
  Property& prop() { return prop_; }

 private:
  void reseat() {
    if (!owned_buffer_.empty()) {
      if (prop_.type() == DataTypeId::kList) {
        prop_.set_list_data(owned_buffer_);
      } else if (prop_.type() == DataTypeId::kVarchar) {
        prop_.set_string_view(owned_buffer_);
      }
    }
  }

  std::string owned_buffer_;
  Property prop_;
};

}  // namespace execution
}  // namespace neug
