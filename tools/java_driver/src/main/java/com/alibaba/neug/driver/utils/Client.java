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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * HTTP client for communicating with the NeuG database server.
 *
 * <p>This class manages HTTP connections using OkHttp and provides synchronous POST operations for
 * sending query requests to the database server.
 */
public class Client {

    private static final String TRANSACTION_ID_HEADER = "X-Transaction-Id";

    private final String baseUri;
    private OkHttpClient httpClient = null;
    private OkHttpClient transactionHttpClient = null;
    private boolean closed = false;

    /**
     * Constructs a new Client with the specified URI and configuration.
     *
     * @param uri the URI of the database server
     * @param config the configuration for connection pooling and timeouts
     */
    public Client(String uri, Config config) {
        this.baseUri =
                (uri != null && uri.endsWith("/")) ? uri.substring(0, uri.length() - 1) : uri;
        this.closed = false;

        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .connectionPool(
                                new ConnectionPool(
                                        config.getMaxConnectionPoolSize(),
                                        config.getKeepAliveIntervalMillis(),
                                        TimeUnit.MILLISECONDS))
                        .retryOnConnectionFailure(true)
                        .connectTimeout(config.getConnectionTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .readTimeout(config.getReadTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .writeTimeout(config.getWriteTimeoutMillis(), TimeUnit.MILLISECONDS);
        httpClient = builder.build();
        transactionHttpClient = httpClient.newBuilder().retryOnConnectionFailure(false).build();
    }

    /**
     * Sends a synchronous POST request to the database server.
     *
     * @param request the request body as a byte array
     * @return the response body as a byte array
     * @throws IOException if an I/O error occurs during the request
     */
    public byte[] syncPost(byte[] request) throws IOException {
        if (closed) {
            throw new IllegalStateException("Client is already closed");
        }
        HttpResponse response = executePost(httpClient, "/cypher", request, null);
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected HTTP status " + response.getStatusCode());
        }
        return response.getBody();
    }

    /** Sends a transaction POST without automatic transport retries. */
    public HttpResponse post(String path, byte[] request, String transactionId) throws IOException {
        return executePost(transactionHttpClient, path, request, transactionId);
    }

    private HttpResponse executePost(
            OkHttpClient client, String path, byte[] request, String transactionId)
            throws IOException {
        if (closed) {
            throw new IllegalStateException("Client is already closed");
        }
        RequestBody body = RequestBody.create(request);
        Request.Builder requestBuilder = new Request.Builder().url(baseUri + path).post(body);
        if (transactionId != null) {
            requestBuilder.header(TRANSACTION_ID_HEADER, transactionId);
        }
        try (Response response = client.newCall(requestBuilder.build()).execute()) {
            ResponseBody responseBody = response.body();
            byte[] responseBytes = responseBody == null ? new byte[0] : responseBody.bytes();
            return new HttpResponse(
                    response.code(), responseBytes, response.header(TRANSACTION_ID_HEADER));
        }
    }

    /**
     * Checks whether this client has been closed.
     *
     * @return {@code true} if the client is closed, {@code false} otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes this client and releases all associated resources.
     *
     * <p>This method evicts all connections from the connection pool and marks the client as
     * closed.
     */
    public void close() {
        if (!closed) {
            httpClient.connectionPool().evictAll();
            httpClient.dispatcher().executorService().shutdown();
            if (httpClient.cache() != null) {
                try {
                    httpClient.cache().close();
                } catch (IOException ignored) {
                    // Ignored: best-effort cache close.
                }
            }
            closed = true;
        }
    }

    /** HTTP response envelope retained for transaction state decisions. */
    public static final class HttpResponse {
        private final int statusCode;
        private final byte[] body;
        private final String transactionId;

        public HttpResponse(int statusCode, byte[] body, String transactionId) {
            this.statusCode = statusCode;
            this.body = body;
            this.transactionId = transactionId;
        }

        public boolean isSuccessful() {
            return statusCode >= 200 && statusCode < 300;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public byte[] getBody() {
            return body;
        }

        public String getTransactionId() {
            return transactionId;
        }
    }
}
