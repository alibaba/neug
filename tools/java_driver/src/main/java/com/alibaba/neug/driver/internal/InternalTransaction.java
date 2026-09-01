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
import com.alibaba.neug.driver.Transaction;
import com.alibaba.neug.driver.utils.Client;
import com.alibaba.neug.driver.utils.QuerySerializer;
import com.alibaba.neug.driver.utils.ResponseParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import okhttp3.HttpUrl;

public class InternalTransaction implements Transaction {

    private enum State {
        ACTIVE,
        ROLLBACK_ONLY,
        TERMINAL_UNKNOWN,
        CLOSED
    }

    private final Client client;
    private final HttpUrl queryEndpoint;
    private final HttpUrl commitEndpoint;
    private final HttpUrl rollbackEndpoint;
    private static final byte[] EMPTY_BODY = new byte[0];
    private State state;

    public InternalTransaction(Client client, String transactionId) {
        this.client = client;
        this.queryEndpoint = client.endpoint("transactions", transactionId, "query");
        this.commitEndpoint = client.endpoint("transactions", transactionId, "commit");
        this.rollbackEndpoint = client.endpoint("transactions", transactionId, "rollback");
        this.state = State.ACTIVE;
    }

    @Override
    public ResultSet run(String statement) {
        return run(statement, null);
    }

    @Override
    public ResultSet run(String statement, Map<String, Object> parameters) {
        ensureRunnable();
        try {
            byte[] request = QuerySerializer.serialize(statement, parameters);
            Client.HttpResponse response = client.syncPost(queryEndpoint, request);
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
        } catch (IOException e) {
            state = State.ROLLBACK_ONLY;
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public void commit() {
        if (state == State.ROLLBACK_ONLY) {
            throw new IllegalStateException("Transaction is rollback-only");
        }
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Transaction is not committable");
        }
        state = State.TERMINAL_UNKNOWN;
        try {
            Client.HttpResponse response = client.syncPost(commitEndpoint, EMPTY_BODY);
            if (!response.isSuccessful()) {
                updateStateAfterTerminalFailure(response);
                throw httpFailure("commit transaction", response);
            }
            state = State.CLOSED;
        } catch (IOException e) {
            throw new RuntimeException("Transaction commit outcome is unknown", e);
        }
    }

    @Override
    public void rollback() {
        if (state != State.ACTIVE && state != State.ROLLBACK_ONLY) {
            throw new IllegalStateException("Transaction is not rollbackable");
        }
        state = State.TERMINAL_UNKNOWN;
        try {
            Client.HttpResponse response = client.syncPost(rollbackEndpoint, EMPTY_BODY);
            if (!response.isSuccessful()) {
                updateStateAfterTerminalFailure(response);
                throw httpFailure("rollback transaction", response);
            }
            state = State.CLOSED;
        } catch (IOException e) {
            throw new RuntimeException("Transaction rollback outcome is unknown", e);
        }
    }

    @Override
    public void close() {
        if (state == State.ACTIVE || state == State.ROLLBACK_ONLY) {
            rollback();
        }
    }

    @Override
    public boolean isOpen() {
        return state == State.ACTIVE || state == State.ROLLBACK_ONLY;
    }

    void ensureSessionReusable() {
        if (state == State.TERMINAL_UNKNOWN) {
            throw new IllegalStateException(
                    "Transaction outcome is unknown; close this session and create a new one");
        }
        if (state != State.CLOSED) {
            throw new IllegalStateException("There is an active transaction");
        }
    }

    private void ensureRunnable() {
        if (state == State.ROLLBACK_ONLY) {
            throw new IllegalStateException("Transaction is rollback-only");
        }
        if (state == State.TERMINAL_UNKNOWN) {
            throw new IllegalStateException("Transaction outcome is unknown");
        }
        if (state == State.CLOSED) {
            throw new IllegalStateException("Transaction is already closed");
        }
    }

    private RuntimeException httpFailure(String operation, Client.HttpResponse response) {
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        return new RuntimeException(
                "Failed to " + operation + ": HTTP " + response.getStatusCode() + " " + body);
    }

    private void updateStateAfterTerminalFailure(Client.HttpResponse response) {
        if (response.getStatusCode() == 409) {
            // The request may have raced with an in-flight operation. The server keeps the
            // transaction, so prevent further queries or commits but allow rollback to be retried.
            state = State.ROLLBACK_ONLY;
        } else if (response.getStatusCode() == 410) {
            // The server confirms that the transaction has expired or no longer exists.
            state = State.CLOSED;
        } else {
            // Other failures do not establish whether the terminal operation took effect.
            state = State.TERMINAL_UNKNOWN;
        }
    }
}
