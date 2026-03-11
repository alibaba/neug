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
package org.alibaba.neug.driver;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

/**
 * A cursor over the results of a database query.
 *
 * <p>A ResultSet maintains a cursor pointing to its current row of data. Initially the cursor is
 * positioned before the first row. The {@link #next()} method moves the cursor to the next row.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ResultSet results = session.run("MATCH (n:Person) RETURN n.name, n.age");
 * while (results.next()) {
 *     String name = results.getString("n.name");
 *     int age = results.getInt("n.age");
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
     */
    Object getObject(String columnName);

    /**
     * Retrieves the value of the designated column as an Object.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value
     */
    Object getObject(int columnIndex);

    /**
     * Retrieves the value of the designated column as an int.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     */
    int getInt(String columnName);

    /**
     * Retrieves the value of the designated column as an int.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     */
    int getInt(int columnIndex);

    /**
     * Retrieves the value of the designated column as a long.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     */
    long getLong(String columnName);

    /**
     * Retrieves the value of the designated column as a long.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     */
    long getLong(int columnIndex);

    /**
     * Retrieves the value of the designated column as a String.
     *
     * @param columnName the name of the column
     * @return the column value; {@code null} if the value is SQL NULL
     */
    String getString(String columnName);

    /**
     * Retrieves the value of the designated column as a String.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; {@code null} if the value is SQL NULL
     */
    String getString(int columnIndex);

    /**
     * Retrieves the value of the designated column as a Date.
     *
     * @param columnName the name of the column
     * @return the column value; {@code null} if the value is SQL NULL
     */
    Date getDate(String columnName);

    /**
     * Retrieves the value of the designated column as a Date.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; {@code null} if the value is SQL NULL
     */
    Date getDate(int columnIndex);

    /**
     * Retrieves the value of the designated column as a Timestamp.
     *
     * @param columnName the name of the column
     * @return the column value; {@code null} if the value is SQL NULL
     */
    Timestamp getTimestamp(String columnName);

    /**
     * Retrieves the value of the designated column as a Timestamp.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; {@code null} if the value is SQL NULL
     */
    Timestamp getTimestamp(int columnIndex);

    /**
     * Retrieves the value of the designated column as a boolean.
     *
     * @param columnName the name of the column
     * @return the column value; {@code false} if the value is SQL NULL
     */
    boolean getBoolean(String columnName);

    /**
     * Retrieves the value of the designated column as a boolean.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; {@code false} if the value is SQL NULL
     */
    boolean getBoolean(int columnIndex);

    /**
     * Retrieves the value of the designated column as a double.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     */
    double getDouble(String columnName);

    /**
     * Retrieves the value of the designated column as a double.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     */
    double getDouble(int columnIndex);

    /**
     * Retrieves the value of the designated column as a float.
     *
     * @param columnName the name of the column
     * @return the column value; 0 if the value is SQL NULL
     */
    float getFloat(String columnName);

    /**
     * Retrieves the value of the designated column as a float.
     *
     * @param columnIndex the column index (0-based)
     * @return the column value; 0 if the value is SQL NULL
     */
    float getFloat(int columnIndex);

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

    /**
     * Retrieves the names of all columns in this ResultSet.
     *
     * @return a list of column names
     */
    List<String> getColumnNames();
}
