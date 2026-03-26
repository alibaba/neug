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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * A cursor over the results of a database query.
 *
 * <p>A ResultSet maintains a cursor pointing to its current row of data. Initially the cursor is
 * positioned before the first row. The {@link #next()} method moves the cursor to the next row.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ResultSet results = session.run("MATCH (n:Person) RETURN n.name as name, n.age as age");
 * while (results.next()) {
 *     String name = results.getString("name");
 *     int age = results.getInt("age");
 *     System.out.println(name + " is " + age + " years old");
 * }
 * results.close();
 * }</pre>
 */
public interface ResultSet extends AutoCloseable {
    /**
     * Moves the cursor forward one row from its current position.
     *
     * @return {@code true} if the new current row is valid; {@code false} if there are no more rows
     */
    boolean next();

    /**
     * Moves the cursor backward one row from its current position.
     *
     * @return {@code true} if the new current row is valid; {@code false} if the cursor is before
     *     the first row
     */
    boolean previous();

    /**
     * Moves the cursor to the given row number in this ResultSet.
     *
     * @param row the row number to move to (0-based)
     * @return {@code true} if the cursor is moved to a valid row; {@code false} otherwise
     */
    boolean absolute(int row);

    /**
     * Moves the cursor a relative number of rows, either positive or negative.
     *
     * @param rows the number of rows to move (positive for forward, negative for backward)
     * @return {@code true} if the cursor is moved to a valid row; {@code false} otherwise
     */
    boolean relative(int rows);

    /**
     * Retrieves the current row number.
     *
     * @return the current row number (0-based), or -1 if there is no current row
     */
    int getRow();

    /**
     * Retrieves the value of the designated column as an Object.
     *
     * @param columnName the name of the column
     * @return the column value
     * @throws IllegalArgumentException if the column name is not valid
     */
    Object getObject(String columnName);

    /**
     * Retrieves the value of the designated column as an Object.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    Object getObject(int columnIndex);

    /**
     * Retrieves the value of the designated column as an int.
     *
     * <p><b>Type requirement:</b> The column must be of type INT32 or compatible numeric type.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of a compatible type
     */
    int getInt(String columnName);

    /**
     * Retrieves the value of the designated column as an int.
     *
     * <p><b>Type requirement:</b> The column must be of type INT32 or compatible numeric type.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of a compatible type
     */
    int getInt(int columnIndex);

    /**
     * Retrieves the value of the designated column as a long.
     *
     * <p><b>Type requirement:</b> The column must be of type INT64 or compatible numeric type.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of a compatible type
     */
    long getLong(String columnName);

    /**
     * Retrieves the value of the designated column as a long.
     *
     * <p><b>Type requirement:</b> The column must be of type INT64 or compatible numeric type.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of a compatible type
     */
    long getLong(int columnIndex);

    /**
     * Retrieves the value of the designated column as a String.
     *
     * <p><b>Type requirement:</b> The column must be of type STRING or a type that can be converted
     * to string.
     *
     * @param columnName the name of the column
     * @return the column value;
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of a compatible type
     */
    String getString(String columnName);

    /**
     * Retrieves the value of the designated column as a String.
     *
     * <p><b>Type requirement:</b> The column must be of type STRING or a type that can be converted
     * to string.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value;
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of a compatible type
     */
    String getString(int columnIndex);

    /**
     * Retrieves the value of the designated column as a Date.
     *
     * <p><b>Type requirement:</b> The column must be of type DATE.
     *
     * @param columnName the name of the column
     * @return the column value;
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of type DATE
     */
    Date getDate(String columnName);

    /**
     * Retrieves the value of the designated column as a Date.
     *
     * <p><b>Type requirement:</b> The column must be of type DATE.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value;
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of type DATE
     */
    Date getDate(int columnIndex);

    /**
     * Retrieves the value of the designated column as a Timestamp.
     *
     * <p><b>Type requirement:</b> The column must be of type TIMESTAMP.
     *
     * @param columnName the name of the column
     * @return the column value;
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of type TIMESTAMP
     */
    Timestamp getTimestamp(String columnName);

    /**
     * Retrieves the value of the designated column as a Timestamp.
     *
     * <p><b>Type requirement:</b> The column must be of type TIMESTAMP.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value;
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of type TIMESTAMP
     */
    Timestamp getTimestamp(int columnIndex);

    /**
     * Retrieves the value of the designated column as a boolean.
     *
     * <p><b>Type requirement:</b> The column must be of type BOOLEAN.
     *
     * @param columnName the name of the column
     * @return the column value;
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of type BOOLEAN
     */
    boolean getBoolean(String columnName);

    /**
     * Retrieves the value of the designated column as a boolean.
     *
     * <p><b>Type requirement:</b> The column must be of type BOOLEAN.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value;
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of type BOOLEAN
     */
    boolean getBoolean(int columnIndex);

    /**
     * Retrieves the value of the designated column as a double.
     *
     * <p><b>Type requirement:</b> The column must be of type DOUBLE or compatible numeric type.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of a compatible type
     */
    double getDouble(String columnName);

    /**
     * Retrieves the value of the designated column as a double.
     *
     * <p><b>Type requirement:</b> The column must be of type DOUBLE or compatible numeric type.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of a compatible type
     */
    double getDouble(int columnIndex);

    /**
     * Retrieves the value of the designated column as a float.
     *
     * <p><b>Type requirement:</b> The column must be of type FLOAT or compatible numeric type.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not of a compatible type
     */
    float getFloat(String columnName);

    /**
     * Retrieves the value of the designated column as a float.
     *
     * <p><b>Type requirement:</b> The column must be of type FLOAT or compatible numeric type.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not of a compatible type
     */
    float getFloat(int columnIndex);

    /**
     * Retrieves the value of the designated column as a BigDecimal.
     *
     * <p><b>Type requirement:</b> The column must be a numeric type (INT32, INT64, FLOAT, DOUBLE).
     *
     * <p>BigDecimal provides arbitrary precision and is ideal for financial calculations or when
     * precision is critical.
     *
     * @param columnName the name of the column
     * @return the column value;
     * @throws IllegalArgumentException if the column name is not valid
     * @throws ClassCastException if the column is not a numeric type
     */
    BigDecimal getBigDecimal(String columnName);

    /**
     * Retrieves the value of the designated column as a BigDecimal.
     *
     * <p><b>Type requirement:</b> The column must be a numeric type (INT32, INT64, FLOAT, DOUBLE).
     *
     * <p>BigDecimal provides arbitrary precision and is ideal for financial calculations or when
     * precision is critical.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value;
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the column is not a numeric type
     */
    BigDecimal getBigDecimal(int columnIndex);

    /**
     * Reports whether the last column read had a value of SQL NULL.
     *
     * @return {@code true} if the last column value read was SQL NULL; {@code false} otherwise
     */
    boolean wasNull();

    /** Closes this ResultSet and releases all associated resources. */
    @Override
    void close();

    /**
     * Checks whether the ResultSet has been closed.
     *
     * @return {@code true} if the ResultSet is closed, {@code false} otherwise
     */
    boolean isClosed();

    /** Moves the cursor to the end of this ResultSet object, just after the last row. */
    void afterLast();

    /** Moves the cursor to the beginning of this ResultSet object, just before the first row. */
    void beforeFirst();

    /**
     * Moves the cursor to the last row in this ResultSet object.
     *
     * @return {@code true} if the cursor is on a valid row; {@code false} if there are no rows in
     *     the result set
     */
    boolean last();

    /**
     * Moves the cursor to the first row in this ResultSet object.
     *
     * @return {@code true} if the cursor is on a valid row; {@code false} if there are no rows in
     *     the result set
     */
    boolean first();

    /**
     * Retrieves whether the cursor is on the last row of this ResultSet object.
     *
     * @return {@code true} if the cursor is on the last row; {@code false} otherwise
     */
    boolean isLast();

    /**
     * Retrieves whether the cursor is on the first row of this ResultSet object.
     *
     * @return {@code true} if the cursor is on the first row; {@code false} otherwise
     */
    boolean isFirst();

    /**
     * Retrieves whether the cursor is before the first row of this ResultSet object.
     *
     * @return {@code true} if the cursor is before the first row; {@code false} otherwise
     */
    boolean isBeforeFirst();

    /**
     * Retrieves whether the cursor is after the last row of this ResultSet object.
     *
     * @return {@code true} if the cursor is after the last row; {@code false} otherwise
     */
    boolean isAfterLast();

    /**
     * Retrieves the number, types and properties of this ResultSet object's columns.
     *
     * @return the description of this ResultSet object's columns
     */
    ResultSetMetaData getMetaData();
}
