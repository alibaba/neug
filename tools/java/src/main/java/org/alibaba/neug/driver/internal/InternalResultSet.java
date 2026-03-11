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
package org.alibaba.neug.driver.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.alibaba.neug.driver.ResultSet;
import org.alibaba.neug.driver.Results;
import org.alibaba.neug.driver.utils.JsonUtil;

public class InternalResultSet implements ResultSet {
    public InternalResultSet(Results.QueryResponse response) {
        this.response = response;
        this.currentIndex = -1;
        this.is_null = false;
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
        return false;
    }

    @Override
    public boolean previous() {
        if (currentIndex - 1 >= 0) {
            currentIndex--;
            return true;
        }
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
        Results.Array array = response.getArrays(columnIndex);
        try {
            return getObject(array, currentIndex, false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get object from column " + columnIndex, e);
        }
    }

    private Object getObject(Results.Array array, int rowIndex, boolean isNullSetted)
            throws Exception {
        switch (array.getTypedArrayCase()) {
            case STRING_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getStringArray().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getStringArray().getValues(rowIndex);
                }
            case INT32_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getInt32Array().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getInt32Array().getValues(rowIndex);
                }
            case INT64_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getInt64Array().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getInt64Array().getValues(rowIndex);
                }
            case BOOL_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getBoolArray().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getBoolArray().getValues(rowIndex);
                }
            case DOUBLE_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getDoubleArray().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getDoubleArray().getValues(rowIndex);
                }
            case TIMESTAMP_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getTimestampArray().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getTimestampArray().getValues(rowIndex);
                }
            case DATE_ARRAY:
                {
                    if (!isNullSetted) {
                        ByteString nullBitmap = array.getDateArray().getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    return array.getDateArray().getValues(rowIndex);
                }
            case LIST_ARRAY:
                {
                    Results.ListArray listArray = array.getListArray();

                    if (!isNullSetted) {
                        ByteString nullBitmap = listArray.getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
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

                    if (!isNullSetted) {
                        ByteString nullBitmap = structArray.getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
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
                    if (!isNullSetted) {
                        ByteString nullBitmap = vertexArray.getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
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
                    if (!isNullSetted) {
                        ByteString nullBitmap = edgeArray.getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
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
                    if (!isNullSetted) {
                        ByteString nullBitmap = pathArray.getValidity();
                        is_null =
                                !nullBitmap.isEmpty()
                                        && (nullBitmap.byteAt(rowIndex / 8) & (1 << (rowIndex % 8)))
                                                == 0;
                    }
                    Map<String, Object> map =
                            mapper.readValue(
                                    pathArray.getValues(rowIndex),
                                    new TypeReference<Map<String, Object>>() {});
                    return map;
                }
            default:
                throw new IllegalArgumentException(
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
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasInt32Array()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type int32");
        }
        Results.Int32Array array = arr.getInt32Array();
        ByteString nullBitmap = array.getValidity();
        int value = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return value;
    }

    @Override
    public long getLong(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getLong(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasInt64Array()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type int64");
        }
        Results.Int64Array array = arr.getInt64Array();
        ByteString nullBitmap = array.getValidity();
        long value = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return value;
    }

    @Override
    public String getString(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getString(columnIndex);
    }

    @Override
    public String getString(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasStringArray()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type string");
        }
        Results.StringArray array = arr.getStringArray();
        ByteString nullBitmap = array.getValidity();
        String value = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return value;
    }

    @Override
    public Date getDate(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasDateArray()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type date");
        }
        Results.DateArray array = arr.getDateArray();
        ByteString nullBitmap = array.getValidity();
        long timestamp = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return new Date(timestamp);
    }

    @Override
    public Timestamp getTimestamp(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasTimestampArray()) {
            throw new IllegalArgumentException(
                    "Column " + columnIndex + " is not of type timestamp");
        }
        Results.TimestampArray array = arr.getTimestampArray();
        ByteString nullBitmap = array.getValidity();
        long timestamp = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return new Timestamp(timestamp);
    }

    @Override
    public boolean getBoolean(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getBoolean(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasBoolArray()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type boolean");
        }
        Results.BoolArray array = arr.getBoolArray();
        ByteString nullBitmap = array.getValidity();
        boolean value = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return value;
    }

    @Override
    public double getDouble(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getDouble(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasDoubleArray()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type double");
        }
        Results.DoubleArray array = arr.getDoubleArray();
        ByteString nullBitmap = array.getValidity();
        double value = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return value;
    }

    @Override
    public float getFloat(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        return getFloat(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) {
        Results.Array arr = response.getArrays(columnIndex);
        if (!arr.hasFloatArray()) {
            throw new IllegalArgumentException("Column " + columnIndex + " is not of type float");
        }
        Results.FloatArray array = arr.getFloatArray();
        ByteString nullBitmap = array.getValidity();
        float value = array.getValues(currentIndex);
        is_null =
                !nullBitmap.isEmpty()
                        && (nullBitmap.byteAt(currentIndex / 8) & (1 << (currentIndex % 8))) == 0;
        return value;
    }

    @Override
    public boolean wasNull() {
        return is_null;
    }

    @Override
    public List<String> getColumnNames() {
        Results.MetaDatas metaDatas = response.getSchema();
        List<String> columnNames = new ArrayList<>();
        for (int i = 0; i < metaDatas.getNameCount(); i++) {
            columnNames.add(metaDatas.getName(i));
        }
        return columnNames;
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
        Results.MetaDatas colum_name = response.getSchema();
        int columnCount = colum_name.getNameCount();
        for (int i = 0; i < columnCount; i++) {
            if (colum_name.getName(i).equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    private Results.QueryResponse response;
    private int currentIndex;
    private boolean is_null;
    private boolean closed;
}
