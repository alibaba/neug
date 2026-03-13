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
 * Enumeration of data types supported by NeuG database.
 *
 * <p>This enum maps to the primitive types defined in the Protocol Buffer schema and provides
 * corresponding JDBC SQL type codes for compatibility with standard database interfaces.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Types type = Types.STRING;
 * int jdbcType = type.getJdbcType(); // Returns java.sql.Types.VARCHAR
 * String typeName = type.getTypeName(); // Returns "STRING"
 * }</pre>
 */
public enum Types {
    /** Any type - represents a value of unknown or dynamic type. */
    ANY("ANY", java.sql.Types.OTHER),

    /** 32-bit signed integer. */
    INT32("INT32", java.sql.Types.INTEGER),

    /** 32-bit unsigned integer. */
    UINT32("UINT32", java.sql.Types.INTEGER),

    /** 64-bit signed integer (long). */
    INT64("INT64", java.sql.Types.BIGINT),

    /** 64-bit unsigned integer (unsigned long). */
    UINT64("UINT64", java.sql.Types.BIGINT),
    /** Boolean value (true/false). */
    BOOLEAN("BOOLEAN", java.sql.Types.BOOLEAN),

    /** 32-bit floating point number. */
    FLOAT("FLOAT", java.sql.Types.FLOAT),

    /** 64-bit floating point number (double precision). */
    DOUBLE("DOUBLE", java.sql.Types.DOUBLE),

    /** Variable-length character string. */
    STRING("STRING", java.sql.Types.VARCHAR),
    /** Fixed-precision decimal number. */
    DECIMAL("DECIMAL", java.sql.Types.DECIMAL),

    /** Date value (year, month, day). */
    DATE("DATE", java.sql.Types.DATE),

    /** Time value (hour, minute, second). */
    TIME("TIME", java.sql.Types.TIME),

    /** Timestamp value (date and time). */
    TIMESTAMP("TIMESTAMP", java.sql.Types.TIMESTAMP),
    /** Binary data (byte array). */
    BYTES("BYTES", java.sql.Types.VARBINARY),

    /** Null value - represents the absence of a value. */
    NULL("NULL", java.sql.Types.NULL),

    /** List/array of values. */
    LIST("LIST", java.sql.Types.ARRAY),

    /** Map/dictionary of key-value pairs. */
    MAP("MAP", java.sql.Types.JAVA_OBJECT),
    /** Graph node/vertex. */
    NODE("NODE", java.sql.Types.JAVA_OBJECT),

    /** Graph edge/relationship. */
    EDGE("EDGE", java.sql.Types.JAVA_OBJECT),

    /** Graph path. */
    PATH("PATH", java.sql.Types.JAVA_OBJECT),

    /** Struct/record type. */
    STRUCT("STRUCT", java.sql.Types.STRUCT),

    /** Interval type - represents a time interval. */
    INTERVAL("INTERVAL", java.sql.Types.VARCHAR),

    /** Other/unknown type. */
    OTHER("OTHER", java.sql.Types.OTHER);

    private final String typeName;
    private final int jdbcType;

    /**
     * Constructs a Types enum value.
     *
     * @param typeName the human-readable name of the type
     * @param jdbcType the corresponding JDBC SQL type code from {@link java.sql.Types}
     */
    Types(String typeName, int jdbcType) {
        this.typeName = typeName;
        this.jdbcType = jdbcType;
    }

    /**
     * Returns the human-readable name of this type.
     *
     * @return the type name as a string
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Returns the JDBC SQL type code for this type.
     *
     * <p>The returned value corresponds to constants defined in {@link java.sql.Types}.
     *
     * @return the JDBC type code
     */
    public int getJdbcType() {
        return jdbcType;
    }

    /**
     * Returns the type name as the string representation.
     *
     * @return the type name
     */
    @Override
    public String toString() {
        return typeName;
    }
}
