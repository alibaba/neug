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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for PROFILE and EXPLAIN support in {@link InternalResultSet}.
 *
 * <p>These tests exercise the profile-related methods on {@link ResultSet}:
 *
 * <ul>
 *   <li>{@link ResultSet#getProfileMetrics()} — structured metrics map
 * </ul>
 *
 * <p>All test data is constructed directly from Protocol Buffers builders, so no live server is
 * required.
 */
public class InternalResultSetProfileTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Build a QueryResponse that carries no ProfileResult (normal query). */
    private Results.QueryResponse buildNormalResponse(int rowCount) {
        java.util.List<String> names = new java.util.ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            names.add("name_" + i);
        }
        Results.StringArray nameArray =
                Results.StringArray.newBuilder().addAllValues(names).build();
        Results.Array nameColumn = Results.Array.newBuilder().setStringArray(nameArray).build();
        Results.MetaDatas schema = Results.MetaDatas.newBuilder().addName("name").build();

        return Results.QueryResponse.newBuilder()
                .addArrays(nameColumn)
                .setSchema(schema)
                .setRowCount(rowCount)
                .build();
    }

    /** Build a single OperatorMetrics protobuf message. */
    private Results.ProfileResult.OperatorMetrics buildOperator(
            long operatorId,
            long parentId,
            String name,
            double elapsedMs,
            long outputRows,
            long... childIds) {
        Results.ProfileResult.OperatorMetrics.Builder builder =
                Results.ProfileResult.OperatorMetrics.newBuilder()
                        .setOperatorId(operatorId)
                        .setParentId(parentId)
                        .setOperatorName(name)
                        .setElapsedMs(elapsedMs)
                        .setOutputRows(outputRows);
        for (long childId : childIds) {
            builder.addChildIds(childId);
        }
        return builder.build();
    }

    /** Build a QueryResponse that includes a ProfileResult (PROFILE or EXPLAIN query). */
    private Results.QueryResponse buildProfileResponse(
            int rowCount,
            double totalElapsedMs,
            long totalOutputRows,
            Results.ProfileResult.OperatorMetrics... operators) {

        Results.ProfileResult.Builder profileBuilder =
                Results.ProfileResult.newBuilder()
                        .setTotalElapsedMs(totalElapsedMs)
                        .setTotalOutputRows(totalOutputRows);
        for (Results.ProfileResult.OperatorMetrics op : operators) {
            profileBuilder.addOperators(op);
        }

        Results.QueryResponse.Builder responseBuilder =
                Results.QueryResponse.newBuilder()
                        .setRowCount(rowCount)
                        .setProfileResult(profileBuilder.build());

        if (rowCount > 0) {
            java.util.List<String> names = new java.util.ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                names.add("name_" + i);
            }
            Results.StringArray nameArray =
                    Results.StringArray.newBuilder().addAllValues(names).build();
            Results.Array nameColumn = Results.Array.newBuilder().setStringArray(nameArray).build();
            Results.MetaDatas schema = Results.MetaDatas.newBuilder().addName("name").build();
            responseBuilder.addArrays(nameColumn).setSchema(schema);
        }

        return responseBuilder.build();
    }

    // -----------------------------------------------------------------------
    // getProfileMetrics() — no profile
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfileMetricsEmptyWhenNoProfile() {
        InternalResultSet rs = new InternalResultSet(buildNormalResponse(3));
        Map<String, Object> metrics = rs.getProfileMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.isEmpty());
    }

    // -----------------------------------------------------------------------
    // getProfileMetrics() — top-level aggregate fields
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfileMetricsTopLevelKeys() {
        Results.ProfileResult.OperatorMetrics op = buildOperator(0, -1, "TableScan", 2.0, 4);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 2.0, 4, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        assertTrue(metrics.containsKey("total_elapsed_ms"));
        assertTrue(metrics.containsKey("total_output_rows"));
        assertTrue(metrics.containsKey("operators"));
    }

    @Test
    public void testGetProfileMetricsTotalElapsedMs() {
        Results.ProfileResult.OperatorMetrics op = buildOperator(0, -1, "TableScan", 3.14, 4);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 3.14, 4, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        double totalElapsed = (double) metrics.get("total_elapsed_ms");
        assertEquals(3.14, totalElapsed, 1e-9);
    }

    @Test
    public void testGetProfileMetricsTotalOutputRows() {
        Results.ProfileResult.OperatorMetrics op = buildOperator(0, -1, "TableScan", 1.0, 42);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 1.0, 42, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        // total_output_rows is uint64 in proto → stored as long in Java
        long totalRows = ((Number) metrics.get("total_output_rows")).longValue();
        assertEquals(42L, totalRows);
    }

    @Test
    public void testGetProfileMetricsTotalOutputRowsZeroForExplain() {
        // EXPLAIN mode: plan only, no rows executed
        Results.ProfileResult.OperatorMetrics op = buildOperator(0, -1, "TableScan", 0.0, 0);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(0, 0.0, 0, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        long totalRows = ((Number) metrics.get("total_output_rows")).longValue();
        assertEquals(0L, totalRows);
    }

    // -----------------------------------------------------------------------
    // getProfileMetrics() — operators list
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfileMetricsSingleOperator() {
        Results.ProfileResult.OperatorMetrics op =
                buildOperator(0, -1, "TableScan[person]", 1.5, 4);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 1.5, 4, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        assertEquals(1, operators.size());
    }

    @Test
    public void testGetProfileMetricsOperatorFields() {
        Results.ProfileResult.OperatorMetrics op =
                buildOperator(1, -1, "TableScan[person]", 2.5, 4);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 2.5, 4, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        Map<String, Object> opMap = operators.get(0);

        assertTrue(opMap.containsKey("operator_id"));
        assertTrue(opMap.containsKey("parent_id"));
        assertTrue(opMap.containsKey("operator_name"));
        assertTrue(opMap.containsKey("elapsed_ms"));
        assertTrue(opMap.containsKey("output_rows"));
        assertTrue(opMap.containsKey("child_ids"));
    }

    @Test
    public void testGetProfileMetricsOperatorFieldValues() {
        Results.ProfileResult.OperatorMetrics op =
                buildOperator(3, -1, "TableScan[person]", 1.23, 7);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(7, 1.23, 7, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        Map<String, Object> opMap = operators.get(0);

        assertEquals(3L, ((Number) opMap.get("operator_id")).longValue());
        assertEquals(-1L, ((Number) opMap.get("parent_id")).longValue());
        assertEquals("TableScan[person]", opMap.get("operator_name"));
        assertEquals(1.23, (double) opMap.get("elapsed_ms"), 1e-9);
        assertEquals(7L, ((Number) opMap.get("output_rows")).longValue());
    }

    @Test
    public void testGetProfileMetricsOperatorNoChildren() {
        Results.ProfileResult.OperatorMetrics op =
                buildOperator(0, -1, "TableScan", 0.5, 4 /* no child ids */);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 0.5, 4, op));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");

        @SuppressWarnings("unchecked")
        List<Long> childIds = (List<Long>) operators.get(0).get("child_ids");
        assertNotNull(childIds);
        assertEquals(0, childIds.size());
    }

    @Test
    public void testGetProfileMetricsOperatorChildIds() {
        // Parent operator has two children
        Results.ProfileResult.OperatorMetrics leaf1 =
                buildOperator(1, 0, "TableScan[person]", 0.8, 4);
        Results.ProfileResult.OperatorMetrics leaf2 =
                buildOperator(2, 0, "TableScan[software]", 0.3, 2);
        Results.ProfileResult.OperatorMetrics root =
                buildOperator(0, -1, "HashJoin", 1.5, 6, 1L, 2L);

        InternalResultSet rs =
                new InternalResultSet(buildProfileResponse(6, 1.5, 6, root, leaf1, leaf2));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        Map<String, Object> rootMap = operators.get(0);

        @SuppressWarnings("unchecked")
        List<Long> childIds = (List<Long>) rootMap.get("child_ids");
        assertEquals(2, childIds.size());
        assertTrue(childIds.contains(1L));
        assertTrue(childIds.contains(2L));
    }

    // -----------------------------------------------------------------------
    // getProfileMetrics() — multi-operator tree
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfileMetricsMultipleOperators() {
        Results.ProfileResult.OperatorMetrics leaf =
                buildOperator(1, 0, "TableScan[person]", 1.0, 4);
        Results.ProfileResult.OperatorMetrics root = buildOperator(0, -1, "Projection", 0.2, 4, 1L);

        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 1.2, 4, root, leaf));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        assertEquals(2, operators.size());
    }

    @Test
    public void testGetProfileMetricsParentChildConsistency() {
        // Build a 3-node tree: root(0) → join(1) → [scan1(2), scan2(3)]
        Results.ProfileResult.OperatorMetrics scan1 =
                buildOperator(2, 1, "TableScan[person]", 1.0, 4);
        Results.ProfileResult.OperatorMetrics scan2 =
                buildOperator(3, 1, "TableScan[software]", 0.5, 2);
        Results.ProfileResult.OperatorMetrics join =
                buildOperator(1, 0, "HashJoin", 1.8, 6, 2L, 3L);
        Results.ProfileResult.OperatorMetrics root = buildOperator(0, -1, "Projection", 0.1, 6, 1L);

        InternalResultSet rs =
                new InternalResultSet(buildProfileResponse(6, 3.4, 6, root, join, scan1, scan2));
        Map<String, Object> metrics = rs.getProfileMetrics();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        assertEquals(4, operators.size());

        // Build an id → operator map for cross-checking
        java.util.Map<Long, Map<String, Object>> byId = new java.util.HashMap<>();
        for (Map<String, Object> op : operators) {
            long id = ((Number) op.get("operator_id")).longValue();
            byId.put(id, op);
        }

        // Every declared child_id should exist as a real operator
        for (Map<String, Object> op : operators) {
            @SuppressWarnings("unchecked")
            List<Long> childIds = (List<Long>) op.get("child_ids");
            for (long childId : childIds) {
                assertTrue(
                        byId.containsKey(childId),
                        "child_id " + childId + " has no matching operator");
            }
        }

        // Every non-root operator's parent_id should point to a real operator
        for (Map<String, Object> op : operators) {
            long parentId = ((Number) op.get("parent_id")).longValue();
            if (parentId != -1L) {
                assertTrue(
                        byId.containsKey(parentId),
                        "parent_id " + parentId + " has no matching operator");
            }
        }
    }

    // -----------------------------------------------------------------------
    // getProfileMetrics() — elapsed_ms and output_rows non-negative
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfileMetricsTimingsNonNegative() {
        Results.ProfileResult.OperatorMetrics leaf = buildOperator(1, 0, "TableScan", 0.7, 4);
        Results.ProfileResult.OperatorMetrics root = buildOperator(0, -1, "Projection", 0.1, 4, 1L);

        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 0.8, 4, root, leaf));
        Map<String, Object> metrics = rs.getProfileMetrics();

        assertTrue(((double) metrics.get("total_elapsed_ms")) >= 0.0);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) metrics.get("operators");
        for (Map<String, Object> op : operators) {
            double elapsed = (double) op.get("elapsed_ms");
            long outputRows = ((Number) op.get("output_rows")).longValue();
            assertTrue(elapsed >= 0.0, "elapsed_ms should be non-negative");
            assertTrue(outputRows >= 0L, "output_rows should be non-negative");
        }
    }

    // -----------------------------------------------------------------------
    // Profile result does not interfere with row access
    // -----------------------------------------------------------------------

    @Test
    public void testProfileResultDoesNotAffectRowAccess() {
        Results.ProfileResult.OperatorMetrics op =
                buildOperator(0, -1, "TableScan[person]", 1.5, 4);
        InternalResultSet rs = new InternalResultSet(buildProfileResponse(4, 1.5, 4, op));

        // All 4 rows should still be accessible
        int count = 0;
        while (rs.next()) {
            assertNotNull(rs.getString(0));
            count++;
        }
        assertEquals(4, count);

        // Profile data is still accessible after iterating rows
        Map<String, Object> metrics = rs.getProfileMetrics();
        assertFalse(metrics.isEmpty());
    }
}
