/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

namespace neug {
class EdgeRecord;
class StorageReadInterface;

namespace execution {
// Compare snapshot-local CSR relationship identity, including parallel edges.
bool same_relationship(const StorageReadInterface& graph,
                       const EdgeRecord& left, const EdgeRecord& right);
}  // namespace execution
}  // namespace neug
