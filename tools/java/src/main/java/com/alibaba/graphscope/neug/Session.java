/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.neug;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * NeuG Java Client implementation.
 * Construct with a server URI and call execute to submit cypher queries.
 */
public class Session implements AutoCloseable {
    private final URI serverUri;
    private final OkHttpClient client;
    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public Session(String uri) {
        this(URI.create(uri), createDefaultClient());
    }

    public Session(String uri, OkHttpClient client) {
        this(URI.create(uri), client);
    }

    public Session(URI uri, OkHttpClient client) {
        this.serverUri = Objects.requireNonNull(uri, "uri");
        this.client = Objects.requireNonNull(client, "client");
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static OkHttpClient createDefaultClient() {
        return new OkHttpClient.Builder().build();
    }

    /**
     * Return the json request without any wrapping.
     * Expect req contains param: "skip_serialize_ret_schema": true
     * 
     * @param req
     * @return
     * @throws IOException
     */
    public NeuGResult submit(String req) throws IOException {
        Request request = new Request.Builder()
                .url(serverUri.toString())
                .post(RequestBody.create(req, JSON))
                .addHeader("Content-Type", "application/json")
                .build();
        return sendRequest(request);
    }

    /**
     * Execute a cypher query with an optional access_mode and return a NeuGResult.
     */
    public NeuGResult execute(String cypher, String accessMode) throws IOException {
        // build JSON payload: {"query": "...", "access_mode": "read"}
        ObjectNode obj = MAPPER.createObjectNode();
        obj.put("query", cypher);
        if (accessMode != null) {
            obj.put("access_mode", accessMode);
        }
        String payload = MAPPER.writeValueAsString(obj);
        RequestBody body = RequestBody.create(payload, JSON);
        Request request = new Request.Builder()
                .url(serverUri.toString())
                .post(body)
                .addHeader("Content-Type", "application/json")
                .build();
        return sendRequest(request);
    }

    /**
     * Shared HTTP execution path for submit() and execute().
     */
    private NeuGResult sendRequest(Request request) throws IOException {
        Response response = client.newCall(request).execute();
        try {
            if (!response.isSuccessful()) {
                response.close();
                throw new IOException("Unexpected HTTP response: " + response.code() + " - " + response.message());
            }
            ResponseBody respBody = response.body();
            if (respBody == null) {
                response.close();
                throw new IOException("Empty response body");
            }

            InputStream in = respBody.byteStream();

            NeuGResult result = new NeuGResult(in, response);
            if (!result.isEmpty() && !result.loadNextBatch()) {
                result.close();
                throw new IOException("No Arrow batches in response");
            }
            return result;
        } catch (IOException e) {
            try {
                response.close();
            } catch (Exception ex) {
                /* ignore */ }
            throw e;
        }
    }

    /**
     * Result wrapper for Arrow stream responses. Supports loading multiple record
     * batches.
     * Caller must call close() when finished.
     */
    public static class NeuGResult implements AutoCloseable {
        private boolean closed = false;
        private final Response response;
        private final ArrowStreamReader reader;
        private VectorSchemaRoot root = null;

        NeuGResult(InputStream in, Response response) {
            if (in == null) {
                this.reader = null;
                this.response = response;
                this.root = null;
                return;
            }
            try {
                int protocol = in.read();
                if (protocol != 0) {
                    in.close();
                    throw new IOException("Unsupported Arrow IPC protocol: " + protocol);
                }
            } catch (IOException e) {
                this.reader = null;
                this.response = response;
                this.root = null;
                return;
            }
            this.reader = new ArrowStreamReader(in, allocator);
            this.response = response;
            this.root = null;
            try {
                this.root = reader.getVectorSchemaRoot();
            } catch (IOException e) {
                System.out.print("Could not get vector schema root");
            }
        }

        public boolean isEmpty() {
            return this.reader == null || this.root == null;
        }

        public VectorSchemaRoot getRoot() {
            return root;
        }

        public boolean loadNextBatch() throws IOException {
            if (isEmpty()) {
                return false;
            }
            return this.reader.loadNextBatch();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            if (root != null) {
                root.close();
            }
            if (reader != null) {
                reader.close();
            }
            response.close();
        }

        public long bytesRead() {
            return this.reader.bytesRead();
        }
    }

    @Override
    public void close() {
        // Evict all connections from the client's pool to avoid leaking resources.
        if (client != null) {
            client.connectionPool().evictAll();
        }
    }
}
