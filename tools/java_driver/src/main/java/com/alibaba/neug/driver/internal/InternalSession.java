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
import com.alibaba.neug.driver.Session;
import com.alibaba.neug.driver.TransactionMode;
import com.alibaba.neug.driver.utils.AccessMode;
import com.alibaba.neug.driver.utils.Client;
import com.alibaba.neug.driver.utils.QuerySerializer;
import com.alibaba.neug.driver.utils.ResponseParser;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Internal implementation of the {@link Session} interface.
 *
 * <p>This class handles query execution by serializing queries, sending them to the database server
 * via HTTP, and parsing the responses into ResultSet objects.
 */
public class InternalSession implements Session {

    private enum State {
        IDLE,
        ACTIVE,
        ROLLBACK_ONLY,
        TERMINAL_UNKNOWN
    }

    private static final Pattern TRANSACTION_ID = Pattern.compile("[A-Za-z0-9_-]{22}");

    private final Client client;
    private boolean closed;
    private State state;
    private String transactionId;

    /**
     * Constructs a new InternalSession with the specified client.
     *
     * @param client the HTTP client used to communicate with the database
     */
    public InternalSession(Client client) {
        this.client = client;
        this.closed = false;
        this.state = State.IDLE;
    }

    @Override
    public void beginTransaction(TransactionMode mode) {
        ensureOpen();
        Objects.requireNonNull(mode, "mode");
        if (state != State.IDLE) {
            throw new IllegalStateException("Session already owns an explicit transaction");
        }
        String value = mode == TransactionMode.READ_ONLY ? "read_only" : "read_write";
        byte[] request = ("{\"mode\":\"" + value + "\"}").getBytes(StandardCharsets.UTF_8);
        try {
            Client.HttpResponse response = client.post("/transactions", request, null);
            if (!response.isSuccessful()) {
                throw httpFailure("begin transaction", response);
            }
            String id = response.getTransactionId();
            if (id == null || !TRANSACTION_ID.matcher(id).matches()) {
                throw new RuntimeException(
                        "Begin transaction response has no valid transaction id");
            }
            transactionId = id;
            state = State.ACTIVE;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }
    }

    @Override
    public void commit() {
        ensureOpen();
        if (state == State.ROLLBACK_ONLY) {
            throw new IllegalStateException("Transaction is rollback-only");
        }
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Session has no committable transaction");
        }
        state = State.TERMINAL_UNKNOWN;
        try {
            Client.HttpResponse response =
                    client.post("/transactions/commit", new byte[0], transactionId);
            if (!response.isSuccessful()) {
                state = State.ROLLBACK_ONLY;
                throw httpFailure("commit transaction", response);
            }
            clearTransaction();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Transaction commit outcome is unknown", e);
        }
    }

    @Override
    public void rollback() {
        ensureOpen();
        if (state != State.ACTIVE && state != State.ROLLBACK_ONLY) {
            throw new IllegalStateException("Session has no rollbackable transaction");
        }
        state = State.TERMINAL_UNKNOWN;
        try {
            Client.HttpResponse response =
                    client.post("/transactions/rollback", new byte[0], transactionId);
            if (!response.isSuccessful()) {
                throw httpFailure("rollback transaction", response);
            }
            clearTransaction();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Transaction rollback outcome is unknown", e);
        }
    }

    @Override
    public boolean hasActiveTransaction() {
        return state == State.ACTIVE || state == State.ROLLBACK_ONLY;
    }

    @Override
    public ResultSet run(String query) {
        return run(query, null, null);
    }

    @Override
    public ResultSet run(String query, Map<String, Object> parameters) {
        return run(query, parameters, null);
    }

    @Override
    public ResultSet run(String query, AccessMode mode) {
        return run(query, null, mode);
    }

    @Override
    public ResultSet run(String query, Map<String, Object> parameters, AccessMode mode) {
        ensureOpen();
        if (state == State.ROLLBACK_ONLY) {
            throw new IllegalStateException("Transaction is rollback-only");
        }
        if (state == State.TERMINAL_UNKNOWN) {
            throw new IllegalStateException("Transaction outcome is unknown; close this session");
        }
        try {
            byte[] request = QuerySerializer.serialize(query, parameters, mode);
            if (state == State.IDLE) {
                return ResponseParser.parse(client.syncPost(request));
            }
            Client.HttpResponse response =
                    client.post("/transactions/query", request, transactionId);
            if (!response.isSuccessful()) {
                state = State.ROLLBACK_ONLY;
                throw httpFailure("execute transaction query", response);
            }
            try {
                return ResponseParser.parse(response.getBody());
            } catch (RuntimeException e) {
                state = State.ROLLBACK_ONLY;
                throw e;
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            if (state == State.ACTIVE) {
                state = State.ROLLBACK_ONLY;
            }
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (state == State.ACTIVE || state == State.ROLLBACK_ONLY) {
            try {
                rollback();
            } catch (RuntimeException ignored) {
                // The server-side absolute deadline is the final cleanup path.
            }
        }
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Session is already closed");
        }
    }

    private void clearTransaction() {
        transactionId = null;
        state = State.IDLE;
    }

    private RuntimeException httpFailure(String operation, Client.HttpResponse response) {
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        return new RuntimeException(
                "Failed to " + operation + ": HTTP " + response.getStatusCode() + " " + body);
    }
}
