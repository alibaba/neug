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

import static org.junit.jupiter.api.Assertions.*;

import com.alibaba.neug.driver.internal.InternalResultSet;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for {@link InternalResultSet}. */
public class InternalResultSetTest {

    private Results.QueryResponse createSampleResponse() {
        // Create a simple response with 3 rows and 2 columns (name: String, age: Int)
        Results.StringArray nameArray =
                Results.StringArray.newBuilder()
                        .addAllValues(Arrays.asList("Alice", "Bob", "Charlie"))
                        .build();

        Results.Int32Array ageArray =
                Results.Int32Array.newBuilder().addAllValues(Arrays.asList(30, 25, 35)).build();

        Results.Array nameColumn = Results.Array.newBuilder().setStringArray(nameArray).build();
        Results.Array ageColumn = Results.Array.newBuilder().setInt32Array(ageArray).build();

        Results.MetaDatas metaDatas =
                Results.MetaDatas.newBuilder().addName("name").addName("age").build();

        return Results.QueryResponse.newBuilder()
                .addArrays(nameColumn)
                .addArrays(ageColumn)
                .setSchema(metaDatas)
                .setRowCount(3)
                .build();
    }

    private InternalResultSet resultSet;

    @BeforeEach
    public void setUp() {
        Results.QueryResponse response = createSampleResponse();
        resultSet = new InternalResultSet(response);
    }

    @Test
    public void testNext() {
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getRow());

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getRow());

        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getRow());

        assertFalse(resultSet.next());
    }

    @Test
    public void testPrevious() {
        resultSet.absolute(2);
        assertEquals(2, resultSet.getRow());

        assertTrue(resultSet.previous());
        assertEquals(1, resultSet.getRow());

        assertTrue(resultSet.previous());
        assertEquals(0, resultSet.getRow());

        assertFalse(resultSet.previous());
    }

    @Test
    public void testAbsolute() {
        assertTrue(resultSet.absolute(0));
        assertEquals(0, resultSet.getRow());

        assertTrue(resultSet.absolute(1));
        assertEquals(1, resultSet.getRow());

        assertTrue(resultSet.absolute(2));
        assertEquals(2, resultSet.getRow());

        assertFalse(resultSet.absolute(3));
        assertFalse(resultSet.absolute(-1));
    }

    @Test
    public void testRelative() {
        resultSet.absolute(0);

        assertTrue(resultSet.relative(1));
        assertEquals(1, resultSet.getRow());

        assertTrue(resultSet.relative(1));
        assertEquals(2, resultSet.getRow());

        assertFalse(resultSet.relative(1));

        assertTrue(resultSet.relative(-1));
        assertEquals(1, resultSet.getRow());
    }

    @Test
    public void testGetString() {
        resultSet.next();
        assertEquals("Alice", resultSet.getString("name"));
        assertEquals("Alice", resultSet.getString(0));
    }

    @Test
    public void testGetInt() {
        resultSet.next();
        assertEquals(30, resultSet.getInt("age"));
        assertEquals(30, resultSet.getInt(1));
    }

    @Test
    public void testClose() {
        assertFalse(resultSet.isClosed());
        resultSet.close();
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testGetInvalidColumn() {
        resultSet.next();
        assertThrows(
                RuntimeException.class,
                () -> {
                    resultSet.getString("nonexistent");
                });
    }

    @Test
    public void testGetInvalidColumnIndex() {
        resultSet.next();
        assertThrows(
                Exception.class,
                () -> {
                    resultSet.getString(99);
                });
    }

    @Test
    public void testWasNull() {
        // Create a response with NULL values
        // Row 0: name="Alice", age=30 (no nulls)
        // Row 1: name=NULL, age=25
        // Row 2: name="Charlie", age=NULL
        Results.StringArray nameArray =
                Results.StringArray.newBuilder()
                        .addAllValues(Arrays.asList("Alice", "", "Charlie"))
                        .setValidity(
                                ByteString.copyFrom(new byte[] {0b00000101})) // bit 1 is 0 (NULL)
                        .build();

        Results.Int32Array ageArray =
                Results.Int32Array.newBuilder()
                        .addAllValues(Arrays.asList(30, 25, 0))
                        .setValidity(
                                ByteString.copyFrom(new byte[] {0b00000011})) // bit 2 is 0 (NULL)
                        .build();

        Results.Array nameColumn = Results.Array.newBuilder().setStringArray(nameArray).build();
        Results.Array ageColumn = Results.Array.newBuilder().setInt32Array(ageArray).build();

        Results.MetaDatas metaDatas =
                Results.MetaDatas.newBuilder().addName("name").addName("age").build();

        Results.QueryResponse response =
                Results.QueryResponse.newBuilder()
                        .addArrays(nameColumn)
                        .addArrays(ageColumn)
                        .setSchema(metaDatas)
                        .setRowCount(3)
                        .build();

        InternalResultSet rs = new InternalResultSet(response);

        // Row 0: both values are not null
        rs.next();
        assertEquals("Alice", rs.getString("name"));
        assertFalse(rs.wasNull());
        assertEquals(30, rs.getInt("age"));
        assertFalse(rs.wasNull());

        // Row 1: name is NULL, age is not null
        rs.next();
        rs.getString("name");
        assertTrue(rs.wasNull());
        assertEquals(25, rs.getInt("age"));
        assertFalse(rs.wasNull());

        // Row 2: name is not null, age is NULL
        rs.next();
        assertEquals("Charlie", rs.getString("name"));
        assertFalse(rs.wasNull());
        rs.getInt("age");
        assertTrue(rs.wasNull());
    }

    @Test
    public void testGetBigDecimal() {
        // Create a response with various numeric types
        Results.Int32Array int32Array =
                Results.Int32Array.newBuilder().addAllValues(Arrays.asList(100, 200, 300)).build();

        Results.Int64Array int64Array =
                Results.Int64Array.newBuilder()
                        .addAllValues(Arrays.asList(1000L, 2000L, 3000L))
                        .build();

        Results.DoubleArray doubleArray =
                Results.DoubleArray.newBuilder()
                        .addAllValues(Arrays.asList(10.5, 20.5, 30.5))
                        .build();

        Results.FloatArray floatArray =
                Results.FloatArray.newBuilder()
                        .addAllValues(Arrays.asList(1.5f, 2.5f, 3.5f))
                        .build();

        Results.Array int32Column = Results.Array.newBuilder().setInt32Array(int32Array).build();
        Results.Array int64Column = Results.Array.newBuilder().setInt64Array(int64Array).build();
        Results.Array doubleColumn = Results.Array.newBuilder().setDoubleArray(doubleArray).build();
        Results.Array floatColumn = Results.Array.newBuilder().setFloatArray(floatArray).build();

        Results.MetaDatas metaDatas =
                Results.MetaDatas.newBuilder()
                        .addName("int32_col")
                        .addName("int64_col")
                        .addName("double_col")
                        .addName("float_col")
                        .build();

        Results.QueryResponse response =
                Results.QueryResponse.newBuilder()
                        .addArrays(int32Column)
                        .addArrays(int64Column)
                        .addArrays(doubleColumn)
                        .addArrays(floatColumn)
                        .setSchema(metaDatas)
                        .setRowCount(3)
                        .build();

        InternalResultSet rs = new InternalResultSet(response);

        // Test first row
        rs.next();
        assertEquals(new BigDecimal(100), rs.getBigDecimal("int32_col"));
        assertEquals(new BigDecimal(1000L), rs.getBigDecimal("int64_col"));
        assertEquals(BigDecimal.valueOf(10.5), rs.getBigDecimal("double_col"));
        assertEquals(BigDecimal.valueOf(1.5f), rs.getBigDecimal("float_col"));

        // Test by column index
        assertEquals(new BigDecimal(100), rs.getBigDecimal(0));
        assertEquals(new BigDecimal(1000L), rs.getBigDecimal(1));
        assertEquals(BigDecimal.valueOf(10.5), rs.getBigDecimal(2));
        assertEquals(BigDecimal.valueOf(1.5f), rs.getBigDecimal(3));
    }

    @Test
    public void testGetBigDecimalWithNull() {
        // Create a response with NULL values
        Results.Int32Array int32Array =
                Results.Int32Array.newBuilder()
                        .addAllValues(Arrays.asList(100, 0, 300))
                        .setValidity(
                                ByteString.copyFrom(new byte[] {0b00000101})) // bit 1 is 0 (NULL)
                        .build();

        Results.Array int32Column = Results.Array.newBuilder().setInt32Array(int32Array).build();

        Results.MetaDatas metaDatas = Results.MetaDatas.newBuilder().addName("value").build();

        Results.QueryResponse response =
                Results.QueryResponse.newBuilder()
                        .addArrays(int32Column)
                        .setSchema(metaDatas)
                        .setRowCount(3)
                        .build();

        InternalResultSet rs = new InternalResultSet(response);

        // Row 0: not null
        rs.next();
        assertEquals(new BigDecimal(100), rs.getBigDecimal("value"));
        assertFalse(rs.wasNull());

        // Row 1: NULL
        rs.next();
        assertEquals(BigDecimal.ZERO, rs.getBigDecimal("value"));
        assertTrue(rs.wasNull());

        // Row 2: not null
        rs.next();
        assertEquals(new BigDecimal(300), rs.getBigDecimal("value"));
        assertFalse(rs.wasNull());
    }

    @Test
    public void testUnsignedIntegerOverflow() {
        // Test uint32 overflow: value 3000000000 > Integer.MAX_VALUE (2147483647)
        // In Java signed int, this would be -1294967296 (negative)
        // We need to cast the long to int to represent the unsigned value
        Results.UInt32Array uint32Array =
                Results.UInt32Array.newBuilder()
                        .addValues((int) 3000000000L) // Value > Integer.MAX_VALUE, stored as
                        // negative int
                        .addValues(2147483647) // Integer.MAX_VALUE
                        .addValues(100)
                        .build();

        // Test uint64 overflow: value > Long.MAX_VALUE (9223372036854775807)
        // Value: 18446744073709551615 (max uint64) would be -1 as signed long
        Results.UInt64Array uint64Array =
                Results.UInt64Array.newBuilder()
                        .addValues(-1L) // This represents 18446744073709551615 as unsigned
                        .addValues(9223372036854775807L) // Long.MAX_VALUE
                        .addValues(1000L)
                        .build();

        Results.Array uint32Column = Results.Array.newBuilder().setUint32Array(uint32Array).build();
        Results.Array uint64Column = Results.Array.newBuilder().setUint64Array(uint64Array).build();

        Results.MetaDatas metaDatas =
                Results.MetaDatas.newBuilder().addName("uint32_col").addName("uint64_col").build();

        Results.QueryResponse response =
                Results.QueryResponse.newBuilder()
                        .addArrays(uint32Column)
                        .addArrays(uint64Column)
                        .setSchema(metaDatas)
                        .setRowCount(3)
                        .build();

        InternalResultSet rs = new InternalResultSet(response);

        // Row 0: Test uint32 overflow (3000000000)
        rs.next();
        long uint32Value = rs.getLong("uint32_col");
        assertEquals(3000000000L, uint32Value, "uint32 value should be 3000000000 (not negative)");

        // Test getBigDecimal for uint32 overflow
        BigDecimal uint32Decimal = rs.getBigDecimal("uint32_col");
        assertEquals(new BigDecimal(3000000000L), uint32Decimal);

        // Test uint64 overflow (max uint64 = 18446744073709551615)
        BigDecimal uint64Decimal = rs.getBigDecimal("uint64_col");
        assertEquals(
                new BigDecimal("18446744073709551615"),
                uint64Decimal,
                "uint64 max value should be 18446744073709551615 (not negative)");

        // Row 1: Test boundary values (MAX_VALUE for signed types)
        rs.next();
        assertEquals(2147483647L, rs.getLong("uint32_col"));
        assertEquals(new BigDecimal(2147483647L), rs.getBigDecimal("uint32_col"));
        assertEquals(9223372036854775807L, rs.getLong("uint64_col"));
        assertEquals(new BigDecimal(9223372036854775807L), rs.getBigDecimal("uint64_col"));

        // Row 2: Test normal values
        rs.next();
        assertEquals(100L, rs.getLong("uint32_col"));
        assertEquals(1000L, rs.getLong("uint64_col"));
    }
}
