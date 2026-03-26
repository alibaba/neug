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
 * <p>This enum represents the native value types exposed by the NeuG Java driver.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Types type = Types.STRING;
 * String typeName = type.getTypeName(); // Returns "STRING"
 * }</pre>
 */
public enum Types {
    /** Any type - represents a value of unknown or dynamic type. */
    ANY("ANY"),

    /** 32-bit signed integer. */
    INT32("INT32"),

    /** 32-bit unsigned integer. */
    UINT32("UINT32"),

    /** 64-bit signed integer (long). */
    INT64("INT64"),

    /** 64-bit unsigned integer (unsigned long). */
    UINT64("UINT64"),
    /** Boolean value (true/false). */
    BOOLEAN("BOOLEAN"),

    /** 32-bit floating point number. */
    FLOAT("FLOAT"),

    /** 64-bit floating point number (double precision). */
    DOUBLE("DOUBLE"),

    /** Variable-length character string. */
    STRING("STRING"),
    /** Fixed-precision decimal number. */
    DECIMAL("DECIMAL"),

    /** Date value (year, month, day). */
    DATE("DATE"),

    /** Time value (hour, minute, second). */
    TIME("TIME"),

    /** Timestamp value (date and time). */
    TIMESTAMP("TIMESTAMP"),
    /** Binary data (byte array). */
    BYTES("BYTES"),

    /** Null value - represents the absence of a value. */
    NULL("NULL"),

    /** List/array of values. */
    LIST("LIST"),

    /** Map/dictionary of key-value pairs. */
    MAP("MAP"),
    /** Graph node/vertex. */
    NODE("NODE"),

    /** Graph edge/relationship. */
    EDGE("EDGE"),

    /** Graph path. */
    PATH("PATH"),

    /** Struct/record type. */
    STRUCT("STRUCT"),

    /** Interval type - represents a time interval. */
    INTERVAL("INTERVAL"),

    /** Other/unknown type. */
    OTHER("OTHER");

    private final String typeName;

    /**
     * Constructs a Types enum value.
     *
     * @param typeName the human-readable name of the type
     */
    Types(String typeName) {
        this.typeName = typeName;
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
     * Returns the type name as the string representation.
     *
     * @return the type name
     */
    @Override
    public String toString() {
        return typeName;
    }
}
