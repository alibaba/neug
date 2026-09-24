/** Copyright 2020 Alibaba Group Holding Limited.
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
#pragma once

#include "neug/main/neug_db.h"

namespace neug {

/**
 * @brief Non-transferable ownership token for a database's TP service mode.
 *
 * The lease keeps TP WAL writers active and prevents local connections or
 * database shutdown until the service runtime has been destroyed. Releasing
 * the lease returns the database to embedded mode.
 */
class NeugDB::ServiceModeLease {
 public:
  ServiceModeLease(const ServiceModeLease&) = delete;
  ServiceModeLease& operator=(const ServiceModeLease&) = delete;
  ServiceModeLease(ServiceModeLease&&) = delete;
  ServiceModeLease& operator=(ServiceModeLease&&) = delete;
  ~ServiceModeLease() noexcept;

 private:
  friend class NeugDB;

  explicit ServiceModeLease(NeugDB& db) noexcept : db_(db) {}

  NeugDB& db_;
};

}  // namespace neug
