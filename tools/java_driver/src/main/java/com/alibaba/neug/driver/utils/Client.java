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

    private final String uri;
    private OkHttpClient httpClient = null;
    private boolean closed = false;

    /**
     * Constructs a new Client with the specified URI and configuration.
     *
     * @param uri the URI of the database server
     * @param config the configuration for connection pooling and timeouts
     */
    public Client(String uri, Config config) {
        this.uri = (uri != null && uri.endsWith("/")) ? uri + "cypher" : uri + "/cypher";
        this.closed = false;

        httpClient =
                new OkHttpClient.Builder()
                        .connectionPool(
                                new ConnectionPool(
                                        config.getMaxConnectionPoolSize(),
                                        config.getKeepAliveIntervalMillis(),
                                        TimeUnit.MILLISECONDS))
                        .retryOnConnectionFailure(true)
                        .connectTimeout(config.getConnectionTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .readTimeout(config.getReadTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .writeTimeout(config.getWriteTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .build();
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
        RequestBody body = RequestBody.create(request);
        Request httpRequest = new Request.Builder().url(uri).post(body).build();
        try (Response response = httpClient.newCall(httpRequest).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            ResponseBody responseBody = response.body();
            if (responseBody == null) {
                throw new IOException("Response body is null");
            }
            return responseBody.bytes();
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
}
