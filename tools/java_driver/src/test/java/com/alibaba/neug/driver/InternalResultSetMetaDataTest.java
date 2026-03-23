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
import com.alibaba.neug.driver.utils.Types;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Test class for {@link ResultSetMetaData}. */
public class InternalResultSetMetaDataTest {

    @Test
    public void testMetaDataReturnsNativeTypesAndColumnProperties() {
        Results.StringArray nameArray =
                Results.StringArray.newBuilder()
                        .addAllValues(Arrays.asList("Alice", "", "Charlie"))
                        .setValidity(ByteString.copyFrom(new byte[] {0b00000101}))
                        .build();

        Results.Int64Array scoreArray =
                Results.Int64Array.newBuilder().addAllValues(Arrays.asList(10L, 20L, 30L)).build();

        Results.BoolArray activeArray =
                Results.BoolArray.newBuilder()
                        .addAllValues(Arrays.asList(true, false, true))
                        .build();

        Results.QueryResponse response =
                Results.QueryResponse.newBuilder()
                        .addArrays(Results.Array.newBuilder().setStringArray(nameArray).build())
                        .addArrays(Results.Array.newBuilder().setInt64Array(scoreArray).build())
                        .addArrays(Results.Array.newBuilder().setBoolArray(activeArray).build())
                        .setSchema(
                                Results.MetaDatas.newBuilder()
                                        .addName("name")
                                        .addName("score")
                                        .addName("active")
                                        .build())
                        .setRowCount(3)
                        .build();

        InternalResultSet resultSet = new InternalResultSet(response);
        ResultSetMetaData metaData = resultSet.getMetaData();

        assertEquals(3, metaData.getColumnCount());

        assertEquals("name", metaData.getColumnName(0));
        assertEquals(Types.STRING, metaData.getColumnType(0));
        assertEquals("STRING", metaData.getColumnTypeName(0));
        assertEquals(1, metaData.isNullable(0));
        assertFalse(metaData.isSigned(0));

        assertEquals("score", metaData.getColumnName(1));
        assertEquals(Types.INT64, metaData.getColumnType(1));
        assertEquals("INT64", metaData.getColumnTypeName(1));
        assertEquals(0, metaData.isNullable(1));
        assertTrue(metaData.isSigned(1));

        assertEquals("active", metaData.getColumnName(2));
        assertEquals(Types.BOOLEAN, metaData.getColumnType(2));
        assertEquals("BOOLEAN", metaData.getColumnTypeName(2));
        assertEquals(0, metaData.isNullable(2));
        assertFalse(metaData.isSigned(2));
    }

    @Test
    public void testMetaDataRejectsInvalidColumnIndex() {
        Results.StringArray nameArray =
                Results.StringArray.newBuilder().addAllValues(Arrays.asList("Alice")).build();

        Results.QueryResponse response =
                Results.QueryResponse.newBuilder()
                        .addArrays(Results.Array.newBuilder().setStringArray(nameArray).build())
                        .setSchema(Results.MetaDatas.newBuilder().addName("name").build())
                        .setRowCount(1)
                        .build();

        ResultSetMetaData metaData = new InternalResultSet(response).getMetaData();

        assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnName(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> metaData.getColumnType(1));
        assertThrows(IndexOutOfBoundsException.class, () -> metaData.isNullable(2));
        assertThrows(IndexOutOfBoundsException.class, () -> metaData.isSigned(3));
    }
}
