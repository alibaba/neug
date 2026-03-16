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
package com.alibaba.neug.driver;

import com.alibaba.neug.driver.utils.Types;

/**
 * Provides information about the types and properties of the columns in a {@link ResultSet}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ResultSet results = session.run("MATCH (n:Person) RETURN n.name, n.age");
 * ResultSetMetaData metaData = results.getMetaData();
 * int columnCount = metaData.getColumnCount();
 * for (int i = 0; i < columnCount; i++) {
 *     System.out.println("Column " + i + ": " + metaData.getColumnName(i) +
 *                        " (type: " + metaData.getColumnTypeName(i) + ")");
 * }
 * }</pre>
 */
public interface ResultSetMetaData {
    /**
     * Returns the number of columns in the result set.
     *
     * @return the number of columns
     */
    int getColumnCount();

    /**
     * Gets the designated column's name.
     *
     * @param column the column index (0-based)
     * @return the column name
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    String getColumnName(int column);

    /**
     * Retrieves the designated column's native NeuG type.
     *
     * @param column the column index (0-based)
     * @return the native NeuG type enum
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    Types getColumnType(int column);

    /**
     * Retrieves the designated column's database-specific type name.
     *
     * @param column the column index (0-based)
     * @return the type name used by the database
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    String getColumnTypeName(int column);

    /**
     * Indicates the nullability of values in the designated column.
     *
     * @param column the column index (0-based)
     * @return the nullability status (0 = no nulls, 1 = nullable, 2 = unknown)
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    int isNullable(int column);

    /**
     * Indicates whether values in the designated column are signed numbers.
     *
     * @param column the column index (0-based)
     * @return {@code true} if the column contains signed numeric values; {@code false} otherwise
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    boolean isSigned(int column);
}
