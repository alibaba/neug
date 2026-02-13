/*
 * Copyright 2024 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.alibaba.graphscope.neug;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;

/**
 * Integration test that sends a cypher query to a local NeuG HTTP endpoint and
 * prints the returned Arrow table.
 *
 * Ensure a server is running at http://localhost:10000/cypher that accepts POST
 * {"query":"...","access_mode":"read"}
 * and responds with Arrow IPC stream bytes.
 */
public class SessionTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testSubmitQueryAndPrintArrowTable() throws Exception {
        String uri = "http://localhost:10000/cypher";
        if (System.getenv("NEUG_SERVER_URI") != null) {
            uri = System.getenv("NEUG_SERVER_URI");
        }
        String cypher = "MATCH(n) return [n.id, n.name] as info limit 10";

        try (Session client = new Session(uri)) {
            Session.NeuGResult result = client.execute(cypher, "read");
            try (Session.NeuGResult r = result) {
                do {
                    VectorSchemaRoot root = r.getRoot();
                    assertNotNull(root);
                    int rows = root.getRowCount();
                    List<FieldVector> vectors = root.getFieldVectors();

                    System.out.println("--- Arrow Batch (rows=" + rows + ") ---");
                    for (int i = 0; i < rows; i++) {
                        StringBuilder sb = new StringBuilder();
                        for (FieldVector v : vectors) {
                            Object val = v.getObject(i);
                            sb.append(v.getName()).append("=").append(val).append(" ");
                        }
                        System.out.println(sb.toString());
                    }
                } while (r.loadNextBatch());
                System.out.println("Total bytes read: " + r.bytesRead());
            }
        } catch (IOException e) {
            // Print error for debugging; rethrow to fail the test
            System.err.println("Failed to query server: " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void testExecuteArrowStream() throws Exception {
        try (MockWebServer server = new MockWebServer();
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            server.start();

            // build a small Arrow table with two columns: id (int) and name (varchar)
            IntVector id = new IntVector("id", allocator);
            id.allocateNew();
            id.setSafe(0, 1);
            id.setSafe(1, 2);
            id.setValueCount(2);

            VarCharVector name = new VarCharVector("name", allocator);
            name.allocateNew();
            name.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));
            name.setSafe(1, "bob".getBytes(StandardCharsets.UTF_8));
            name.setValueCount(2);

            List vectors = Arrays.asList(id, name);
            VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(id.getField(), name.getField()), vectors, 2);

            byte[] bytes;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                baos.write(0);
                writer.start();
                writer.writeBatch();
                writer.end();
                writer.close();
                bytes = baos.toByteArray();
            }

            // clean up writer-side vectors/root
            root.close();
            id.close();
            name.close();

            // enqueue the Arrow IPC bytes as the HTTP response body
            MockResponse resp = new MockResponse()
                    .addHeader("Content-Type", "application/octet-stream")
                    .setBody(new Buffer().write(bytes));
            server.enqueue(resp);

            String baseUrl = server.url("/query").toString();
            Session client = new Session(baseUrl);

            Session.NeuGResult result = client.execute("MATCH (n) RETURN n", "read");
            try {
                VectorSchemaRoot outRoot = result.getRoot();
                assertNotNull(outRoot);
                // first batch was already loaded by execute()
                assertEquals(2, outRoot.getRowCount());

                IntVector outId = (IntVector) outRoot.getVector("id");
                VarCharVector outName = (VarCharVector) outRoot.getVector("name");

                assertEquals(1, outId.get(0));
                assertEquals(2, outId.get(1));

                assertEquals("alice", outName.getObject(0).toString());
                assertEquals("bob", outName.getObject(1).toString());

                // there should be no more batches
                boolean hasMore = result.loadNextBatch();
                assertEquals(false, hasMore);
            } finally {
                result.close();
                client.close();
                server.shutdown();
            }
        }
    }
}
