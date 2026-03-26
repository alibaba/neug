/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.neug.driver.utils;

/**
 * Enumeration of access modes for database operations.
 *
 * <p>The access mode indicates the type of operation being performed on the database:
 *
 * <ul>
 *   <li>{@link #READ} - Read-only operations (queries)
 *   <li>{@link #INSERT} - Insert operations
 *   <li>{@link #UPDATE} - Update operations
 *   <li>{@link #SCHEMA} - Schema modification operations
 * </ul>
 */
public enum AccessMode {
    /** Read-only access mode for query operations. */
    READ,
    /** Insert access mode for adding new data. */
    INSERT,
    /** Update access mode for modifying existing data. */
    UPDATE,
    /** Schema access mode for DDL operations. */
    SCHEMA,
}
