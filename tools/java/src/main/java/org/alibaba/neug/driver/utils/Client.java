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
package org.alibaba.neug.driver.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class Client {

    private final String uri;
    private static OkHttpClient httpClient = null;
    private boolean closed = false;

    public Client(String uri, Config config) {
        this.uri = uri;
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

    public byte[] syncPost(byte[] request) throws IOException {
        RequestBody body = RequestBody.create(request);
        Request httpRequest = new Request.Builder().url(uri).post(body).build();
        try (Response response = httpClient.newCall(httpRequest).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            return response.body().bytes();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        if (!closed) {
            httpClient.connectionPool().evictAll();
            closed = true;
        }
    }
}
