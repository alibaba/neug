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
import okhttp3.HttpUrl;
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

    private final HttpUrl baseUrl;
    private OkHttpClient httpClient = null;
    private boolean closed = false;

    /**
     * Constructs a new Client with the specified URI and configuration.
     *
     * @param uri the URI of the database server
     * @param config the configuration for connection pooling and timeouts
     */
    public Client(String uri, Config config) {
        if (uri == null) {
            throw new IllegalArgumentException("URI must not be null");
        }
        this.baseUrl = HttpUrl.get(uri);
        this.closed = false;

        httpClient =
                new OkHttpClient.Builder()
                        .connectionPool(
                                new ConnectionPool(
                                        config.getMaxConnectionPoolSize(),
                                        config.getKeepAliveIntervalMillis(),
                                        TimeUnit.MILLISECONDS))
                        // Every NeuG endpoint uses POST and may mutate database state. Without a
                        // protocol-level request ID, replaying a request after a connection failure
                        // could execute it twice, so the driver deliberately uses at-most-once
                        // delivery for autocommit and explicit-transaction requests alike.
                        .retryOnConnectionFailure(false)
                        .connectTimeout(config.getConnectionTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .readTimeout(config.getReadTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .writeTimeout(config.getWriteTimeoutMillis(), TimeUnit.MILLISECONDS)
                        .build();
    }

    /**
     * Builds an endpoint relative to the database server's base URL. Each path segment is encoded
     * independently, so identifiers such as transaction IDs can be passed without manually escaping
     * them. Callers should cache the returned URL instead of rebuilding it for every request.
     *
     * @param pathSegments endpoint path segments, for example {@code "transactions", transactionId,
     *     "commit"}
     * @return the encoded endpoint URL
     */
    public HttpUrl endpoint(String... pathSegments) {
        HttpUrl.Builder urlBuilder = baseUrl.newBuilder();
        for (String pathSegment : pathSegments) {
            if (pathSegment == null || pathSegment.isEmpty()) {
                throw new IllegalArgumentException("Path segments must not be null or empty");
            }
            urlBuilder.addPathSegment(pathSegment);
        }
        return urlBuilder.build();
    }

    /**
     * Sends a synchronous POST request to a pre-built endpoint.
     *
     * <p>This method returns both the HTTP status code and response body. Callers are responsible
     * for interpreting non-success status codes according to the operation's semantics.
     *
     * @param url the endpoint URL, normally built once with {@link #endpoint(String...)} and cached
     *     by the caller
     * @param request the request body as a byte array
     * @return the HTTP response containing the status code and response body
     * @throws IOException if the request cannot be sent or the response cannot be read
     * @throws IllegalStateException if this client has already been closed
     */
    public HttpResponse syncPost(HttpUrl url, byte[] request) throws IOException {
        if (closed) {
            throw new IllegalStateException("Client is already closed");
        }
        RequestBody body = RequestBody.create(request);
        Request httpRequest = new Request.Builder().url(url).post(body).build();
        try (Response response = httpClient.newCall(httpRequest).execute()) {
            ResponseBody responseBody = response.body();
            byte[] responseBytes = responseBody == null ? new byte[0] : responseBody.bytes();
            return new HttpResponse(response.code(), responseBytes);
        }
    }

    /** HTTP response envelope retained for callers that need to inspect non-success responses. */
    public static final class HttpResponse {
        private final int statusCode;
        private final byte[] body;

        public HttpResponse(int statusCode, byte[] body) {
            this.statusCode = statusCode;
            this.body = body;
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
