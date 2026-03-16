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
package com.alibaba.neug.driver.internal;

import com.alibaba.neug.driver.ResultSetMetaData;
import com.alibaba.neug.driver.utils.Types;
import java.util.List;

/**
 * Internal implementation of {@link ResultSetMetaData} that provides metadata information about the
 * columns in a result set.
 *
 * <p>This class stores metadata for all columns including their names, types, nullability, and sign
 * information. It provides methods to query this information for individual columns.
 *
 * <p>Column indices are 0-based internally but the interface methods use standard JDBC conventions.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ResultSetMetaData metaData = resultSet.getMetaData();
 * int columnCount = metaData.getColumnCount();
 * for (int i = 0; i < columnCount; i++) {
 *     String name = metaData.getColumnName(i);
 *     String typeName = metaData.getColumnTypeName(i);
 *     System.out.println(name + ": " + typeName);
 * }
 * }</pre>
 *
 * @see ResultSetMetaData
 * @see Types
 */
public class InternalResultSetMetaData implements ResultSetMetaData {
    /**
     * Constructs a new InternalResultSetMetaData with the specified column metadata.
     *
     * @param columnNames the list of column names
     * @param columnNullability the list of column nullability values (from {@link
     *     ResultSetMetaData})
     * @param columnTypes the list of column types
     * @param columnSigned the list of boolean values indicating if columns are signed
     */
    public InternalResultSetMetaData(
            List<String> columnNames,
            List<Integer> columnNullability,
            List<Types> columnTypes,
            List<Boolean> columnSigned) {
        this.columnNames = columnNames;
        this.columnNullability = columnNullability;
        this.columnTypes = columnTypes;
        this.columnSigned = columnSigned;
    }

    /**
     * Validates that the given column index is within valid bounds.
     *
     * @param column the column index to validate
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    private void validateColumnIndex(int column) {
        if (column < 0 || column >= columnNames.size()) {
            throw new IndexOutOfBoundsException("Invalid column index: " + column);
        }
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public String getColumnName(int column) {
        validateColumnIndex(column);
        return columnNames.get(column);
    }

    @Override
    public Types getColumnType(int column) {
        validateColumnIndex(column);
        return columnTypes.get(column);
    }

    @Override
    public boolean isSigned(int column) {
        validateColumnIndex(column);
        return columnSigned.get(column);
    }

    @Override
    public int isNullable(int column) {
        validateColumnIndex(column);
        return columnNullability.get(column);
    }

    @Override
    public String getColumnTypeName(int column) {
        validateColumnIndex(column);
        return columnTypes.get(column).name();
    }

    /** The list of column names in the result set. */
    private List<String> columnNames;

    /** The list of column nullability values. */
    private List<Integer> columnNullability;

    /** The list of column data types. */
    private List<Types> columnTypes;

    /** The list indicating whether each column contains signed numeric values. */
    private List<Boolean> columnSigned;
}
