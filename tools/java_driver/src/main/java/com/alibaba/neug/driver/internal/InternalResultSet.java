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

import com.alibaba.neug.driver.ResultSet;
import com.alibaba.neug.driver.ResultSetMetaData;
import com.alibaba.neug.driver.Results;
import com.alibaba.neug.driver.utils.JsonUtil;
import com.alibaba.neug.driver.utils.Types;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Internal implementation of the {@link ResultSet} interface.
 *
 * <p>This class provides access to query results returned from the database server. It wraps a
 * Protocol Buffers QueryResponse object and provides methods to navigate through rows and extract
 * column values in various data types.
 *
 * <p>The ResultSet maintains a cursor position and supports both forward and backward navigation,
 * as well as absolute and relative positioning.
 */
public class InternalResultSet implements ResultSet {
    /**
     * Constructs a new InternalResultSet from a Protocol Buffers query response.
     *
     * @param response the query response from the database server
     */
    public InternalResultSet(Results.QueryResponse response) {
        this.response = response;
        this.currentIndex = -1;
        this.was_null = false;
        this.closed = false;
    }

    @Override
    public boolean absolute(int row) {
        if (row < 0 || row >= response.getRowCount()) {
            return false;
        }
        currentIndex = row;
        return true;
    }

    @Override
    public boolean relative(int rows) {
        return absolute(currentIndex + rows);
    }

    @Override
    public boolean next() {
        if (currentIndex + 1 < response.getRowCount()) {
            currentIndex++;
            return true;
        }
        currentIndex = response.getRowCount(); // move to after-last position
        return false;
    }

    public boolean previous() {
        if (currentIndex - 1 >= 0) {
            currentIndex--;
            return true;
        }
        currentIndex = -1; // move to before-first position
        return false;
    }

    @Override
    public int getRow() {
        return currentIndex;
    }

    @Override
    public Object getObject(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) {
        // Return the appropriate type based on the array type
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array array = response.getArrays(columnIndex);
        try {
            return getObject(array, currentIndex, false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get object from column " + columnIndex, e);
        }
    }

    private void update_was_null(ByteString nullBitmap) {
        was_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
    }

    private Object getObject(Results.Array array, int rowIndex, boolean nullAlreadyHandled)
            throws Exception {
        switch (array.getTypedArrayCase()) {
            case STRING_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getStringArray().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getStringArray().getValues(rowIndex);
                }
            case INT32_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getInt32Array().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getInt32Array().getValues(rowIndex);
                }
            case INT64_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getInt64Array().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getInt64Array().getValues(rowIndex);
                }
            case BOOL_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getBoolArray().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getBoolArray().getValues(rowIndex);
                }
            case FLOAT_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getFloatArray().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getFloatArray().getValues(rowIndex);
                }
            case DOUBLE_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getDoubleArray().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getDoubleArray().getValues(rowIndex);
                }
            case TIMESTAMP_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getTimestampArray().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getTimestampArray().getValues(rowIndex);
                }
            case DATE_ARRAY:
                {
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = array.getDateArray().getValidity();
                        update_was_null(nullBitmap);
                    }
                    return array.getDateArray().getValues(rowIndex);
                }
            case LIST_ARRAY:
                {
                    Results.ListArray listArray = array.getListArray();

                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = listArray.getValidity();
                        update_was_null(nullBitmap);
                    }

                    int start = listArray.getOffsets(rowIndex);
                    int end = listArray.getOffsets(rowIndex + 1);
                    List<Object> list = new ArrayList<>(end - start);
                    for (int i = start; i < end; i++) {
                        list.add(getObject(listArray.getElements(), i, true));
                    }
                    return list;
                }
            case STRUCT_ARRAY:
                {
                    Results.StructArray structArray = array.getStructArray();

                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = structArray.getValidity();
                        update_was_null(nullBitmap);
                    }
                    List<Object> struct = new ArrayList<>(structArray.getFieldsCount());
                    for (int i = 0; i < structArray.getFieldsCount(); i++) {
                        struct.add(getObject(structArray.getFields(i), rowIndex, true));
                    }
                    return struct;
                }
            case VERTEX_ARRAY:
                {
                    Results.VertexArray vertexArray = array.getVertexArray();
                    ObjectMapper mapper = JsonUtil.getInstance();
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = vertexArray.getValidity();
                        update_was_null(nullBitmap);
                    }
                    Map<String, Object> map =
                            mapper.readValue(
                                    vertexArray.getValues(rowIndex),
                                    new TypeReference<Map<String, Object>>() {});
                    return map;
                }
            case EDGE_ARRAY:
                {
                    Results.EdgeArray edgeArray = array.getEdgeArray();
                    ObjectMapper mapper = JsonUtil.getInstance();
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = edgeArray.getValidity();
                        update_was_null(nullBitmap);
                    }
                    Map<String, Object> map =
                            mapper.readValue(
                                    edgeArray.getValues(rowIndex),
                                    new TypeReference<Map<String, Object>>() {});
                    return map;
                }
            case PATH_ARRAY:
                {
                    Results.PathArray pathArray = array.getPathArray();
                    ObjectMapper mapper = JsonUtil.getInstance();
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = pathArray.getValidity();
                        update_was_null(nullBitmap);
                    }
                    Map<String, Object> map =
                            mapper.readValue(
                                    pathArray.getValues(rowIndex),
                                    new TypeReference<Map<String, Object>>() {});
                    return map;
                }
            case INTERVAL_ARRAY:
                {
                    Results.IntervalArray intervalArray = array.getIntervalArray();
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = intervalArray.getValidity();
                        update_was_null(nullBitmap);
                    }
                    return intervalArray.getValues(rowIndex);
                }
            case UINT32_ARRAY:
                {
                    Results.UInt32Array uint32Array = array.getUint32Array();
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = uint32Array.getValidity();
                        update_was_null(nullBitmap);
                    }
                    // Convert uint32 to long to avoid overflow
                    return Integer.toUnsignedLong(uint32Array.getValues(rowIndex));
                }
            case UINT64_ARRAY:
                {
                    Results.UInt64Array uint64Array = array.getUint64Array();
                    if (!nullAlreadyHandled) {
                        ByteString nullBitmap = uint64Array.getValidity();
                        update_was_null(nullBitmap);
                    }
                    // Convert uint64 to BigInteger to avoid overflow
                    long value = uint64Array.getValues(rowIndex);
                    return new BigInteger(Long.toUnsignedString(value));
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported array type: " + array.getTypedArrayCase());
        }
    }

    @Override
    public int getInt(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getInt(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (arr.hasInt32Array()) {
            Results.Int32Array array = arr.getInt32Array();
            ByteString nullBitmap = array.getValidity();
            int value = array.getValues(currentIndex);
            update_was_null(nullBitmap);
            return value;
        }
        return getNumericValue(arr).intValue();
    }

    @Override
    public long getLong(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getLong(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (arr.hasInt64Array()) {
            Results.Int64Array array = arr.getInt64Array();
            ByteString nullBitmap = array.getValidity();
            long value = array.getValues(currentIndex);
            update_was_null(nullBitmap);
            return value;
        }
        return getNumericValue(arr).longValue();
    }

    @Override
    public String getString(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getString(columnIndex);
    }

    @Override
    public String getString(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasStringArray()) {
            return getObject(columnIndex).toString();
        }
        Results.StringArray array = arr.getStringArray();
        ByteString nullBitmap = array.getValidity();
        String value = array.getValues(currentIndex);
        update_was_null(nullBitmap);
        return value;
    }

    @Override
    public Date getDate(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasDateArray()) {
            throw new ClassCastException("Column " + columnIndex + " is not of type date");
        }
        Results.DateArray array = arr.getDateArray();
        ByteString nullBitmap = array.getValidity();
        long timestamp = array.getValues(currentIndex);
        update_was_null(nullBitmap);
        return new Date(timestamp);
    }

    @Override
    public Timestamp getTimestamp(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasTimestampArray()) {
            throw new ClassCastException("Column " + columnIndex + " is not of type timestamp");
        }
        Results.TimestampArray array = arr.getTimestampArray();
        ByteString nullBitmap = array.getValidity();
        long timestamp = array.getValues(currentIndex);
        update_was_null(nullBitmap);
        return new Timestamp(timestamp);
    }

    @Override
    public boolean getBoolean(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getBoolean(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasBoolArray()) {
            throw new ClassCastException("Column " + columnIndex + " is not of type boolean");
        }
        Results.BoolArray array = arr.getBoolArray();
        ByteString nullBitmap = array.getValidity();
        boolean value = array.getValues(currentIndex);
        update_was_null(nullBitmap);
        return value;
    }

    @Override
    public double getDouble(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getDouble(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (arr.hasFloatArray()) {
            Results.FloatArray array = arr.getFloatArray();
            ByteString nullBitmap = array.getValidity();
            float value = array.getValues(currentIndex);
            update_was_null(nullBitmap);
            return value;
        }
        return getNumericValue(arr).doubleValue();
    }

    @Override
    public float getFloat(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getFloat(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        if (arr.hasFloatArray()) {
            Results.FloatArray array = arr.getFloatArray();
            ByteString nullBitmap = array.getValidity();
            float value = array.getValues(currentIndex);
            update_was_null(nullBitmap);
            return value;
        }
        return getNumericValue(arr).floatValue();
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        checkRowIndex();
        checkIndex(columnIndex);
        Results.Array arr = response.getArrays(columnIndex);
        Number value = getNumericValue(arr);
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else if (value instanceof Integer || value instanceof Long) {
            return new BigDecimal(value.longValue());
        } else if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(value.doubleValue());
        }
        throw new ClassCastException(
                "Column " + columnIndex + " cannot be converted to BigDecimal");
    }

    @Override
    public boolean wasNull() {
        return was_null;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private int getColumnIndex(String columnName) {
        Results.MetaDatas metaDatas = response.getSchema();
        int columnCount = metaDatas.getNameCount();
        for (int i = 0; i < columnCount; i++) {
            if (metaDatas.getName(i).equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    private void checkIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= response.getArraysCount()) {
            throw new IndexOutOfBoundsException("Invalid column index: " + columnIndex);
        }
    }

    private void checkRowIndex() {
        if (currentIndex < 0 || currentIndex >= response.getRowCount()) {
            throw new IndexOutOfBoundsException("Cursor is not positioned on a valid row");
        }
    }

    /**
     * Generic method to extract numeric value from any numeric array type.
     *
     * @param arr the array to extract from
     * @return the numeric value as a Number object
     */
    private Number getNumericValue(Results.Array arr) {
        ByteString nullBitmap;

        if (arr.hasInt32Array()) {
            Results.Int32Array array = arr.getInt32Array();
            nullBitmap = array.getValidity();
            update_was_null(nullBitmap);
            return array.getValues(currentIndex);
        }

        if (arr.hasInt64Array()) {
            Results.Int64Array array = arr.getInt64Array();
            nullBitmap = array.getValidity();
            update_was_null(nullBitmap);
            return array.getValues(currentIndex);
        }

        if (arr.hasFloatArray()) {
            Results.FloatArray array = arr.getFloatArray();
            nullBitmap = array.getValidity();
            update_was_null(nullBitmap);
            return array.getValues(currentIndex);
        }

        if (arr.hasDoubleArray()) {
            Results.DoubleArray array = arr.getDoubleArray();
            nullBitmap = array.getValidity();
            update_was_null(nullBitmap);
            return array.getValues(currentIndex);
        }

        if (arr.hasUint32Array()) {
            Results.UInt32Array array = arr.getUint32Array();
            nullBitmap = array.getValidity();
            update_was_null(nullBitmap);
            // Convert unsigned int32 to signed long to avoid overflow
            return Integer.toUnsignedLong(array.getValues(currentIndex));
        }

        if (arr.hasUint64Array()) {
            Results.UInt64Array array = arr.getUint64Array();
            nullBitmap = array.getValidity();
            update_was_null(nullBitmap);
            // Convert unsigned int64 to BigInteger to avoid overflow
            long value = array.getValues(currentIndex);
            return new BigInteger(Long.toUnsignedString(value));
        }

        throw new ClassCastException("Column is not a numeric type");
    }

    @Override
    public void afterLast() {
        // Position the cursor just after the last row
        currentIndex = response.getRowCount();
    }

    @Override
    public void beforeFirst() {
        currentIndex = -1;
    }

    @Override
    public boolean first() {
        if (response.getRowCount() == 0) {
            return false;
        }
        currentIndex = 0;
        return true;
    }

    @Override
    public boolean last() {
        currentIndex = response.getRowCount() - 1;
        return currentIndex >= 0;
    }

    @Override
    public boolean isFirst() {
        return currentIndex == 0 && response.getRowCount() != 0;
    }

    @Override
    public boolean isLast() {
        return currentIndex == response.getRowCount() - 1 && response.getRowCount() != 0;
    }

    @Override
    public boolean isBeforeFirst() {
        return currentIndex == -1 && response.getRowCount() != 0;
    }

    @Override
    public boolean isAfterLast() {
        return currentIndex == response.getRowCount() && response.getRowCount() != 0;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        Results.MetaDatas metaDatas = response.getSchema();
        List<String> columnNames = new ArrayList<>();
        List<Integer> columnNullability = new ArrayList<>();
        List<Types> columnTypes = new ArrayList<>();
        List<Boolean> columnSigned = new ArrayList<>();
        for (int i = 0; i < metaDatas.getNameCount(); i++) {
            columnNames.add(metaDatas.getName(i));
            switch (response.getArrays(i).getTypedArrayCase()) {
                case STRING_ARRAY:
                    columnTypes.add(Types.STRING);
                    columnNullability.add(
                            response.getArrays(i).getStringArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case INT32_ARRAY:
                    columnTypes.add(Types.INT32);
                    columnNullability.add(
                            response.getArrays(i).getInt32Array().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(true);
                    break;
                case INT64_ARRAY:
                    columnTypes.add(Types.INT64);
                    columnNullability.add(
                            response.getArrays(i).getInt64Array().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(true);
                    break;
                case BOOL_ARRAY:
                    columnTypes.add(Types.BOOLEAN);
                    columnNullability.add(
                            response.getArrays(i).getBoolArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case DOUBLE_ARRAY:
                    columnTypes.add(Types.DOUBLE);
                    columnNullability.add(
                            response.getArrays(i).getDoubleArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(true);
                    break;
                case FLOAT_ARRAY:
                    columnTypes.add(Types.FLOAT);
                    columnNullability.add(
                            response.getArrays(i).getFloatArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(true);
                    break;
                case TIMESTAMP_ARRAY:
                    columnTypes.add(Types.TIMESTAMP);
                    columnNullability.add(
                            response.getArrays(i).getTimestampArray().getValidity().isEmpty()
                                    ? 0
                                    : 1);
                    columnSigned.add(false);
                    break;
                case DATE_ARRAY:
                    columnTypes.add(Types.DATE);
                    columnNullability.add(
                            response.getArrays(i).getDateArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case UINT32_ARRAY:
                    columnTypes.add(Types.UINT32);
                    columnNullability.add(
                            response.getArrays(i).getUint32Array().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case UINT64_ARRAY:
                    columnTypes.add(Types.UINT64);
                    columnNullability.add(
                            response.getArrays(i).getUint64Array().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case LIST_ARRAY:
                    columnTypes.add(Types.LIST);
                    columnNullability.add(
                            response.getArrays(i).getListArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case STRUCT_ARRAY:
                    columnTypes.add(Types.STRUCT);
                    columnNullability.add(
                            response.getArrays(i).getStructArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case VERTEX_ARRAY:
                    columnTypes.add(Types.NODE);
                    columnNullability.add(
                            response.getArrays(i).getVertexArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case EDGE_ARRAY:
                    columnTypes.add(Types.EDGE);
                    columnNullability.add(
                            response.getArrays(i).getEdgeArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case PATH_ARRAY:
                    columnTypes.add(Types.PATH);
                    columnNullability.add(
                            response.getArrays(i).getPathArray().getValidity().isEmpty() ? 0 : 1);
                    columnSigned.add(false);
                    break;
                case INTERVAL_ARRAY:
                    columnTypes.add(Types.INTERVAL);
                    columnNullability.add(
                            response.getArrays(i).getIntervalArray().getValidity().isEmpty()
                                    ? 0
                                    : 1);
                    columnSigned.add(false);
                    break;
                default:
                    // For complex types, we can set type as OTHER and nullability as unknown
                    columnTypes.add(Types.OTHER);
                    columnNullability.add(2); // unknown
                    columnSigned.add(false);
            }
        }
        return new InternalResultSetMetaData(
                columnNames, columnNullability, columnTypes, columnSigned);
    }

    private Results.QueryResponse response;
    private int currentIndex;
    private boolean was_null;
    private boolean closed;
}
