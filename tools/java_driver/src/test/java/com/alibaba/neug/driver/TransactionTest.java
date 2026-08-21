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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.neug.driver.internal.InternalSession;
import com.alibaba.neug.driver.utils.Client;
import com.alibaba.neug.driver.utils.Config;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import okhttp3.HttpUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests explicit transaction ownership and terminal state handling. */
public class TransactionTest {

    private static final String TRANSACTION_ID = "ABCDEFGHIJKLMNOPQRSTUV";

    private FakeClient client;

    @BeforeEach
    public void setUp() {
        client = new FakeClient();
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Test
    public void sessionAllowsOnlyOneActiveTransaction() {
        InternalSession session = new InternalSession(client);

        session.beginTransaction();

        assertThrows(IllegalStateException.class, session::beginTransaction);
        assertThrows(IllegalStateException.class, () -> session.run("RETURN 1"));
    }

    @Test
    public void commitReleasesSessionForNextTransaction() {
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        transaction.commit();

        assertFalse(transaction.isOpen());
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void lostCommitResponseBlocksFurtherSessionUse() {
        client.failCommit = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        assertThrows(RuntimeException.class, transaction::commit);

        assertFalse(transaction.isOpen());
        assertThrows(IllegalStateException.class, session::beginTransaction);
        assertThrows(IllegalStateException.class, () -> session.run("RETURN 1"));
    }

    private static final class FakeClient extends Client {
        private boolean failCommit;

        private FakeClient() {
            super("http://localhost", Config.builder().build());
        }

        @Override
        public HttpResponse syncPost(HttpUrl url, byte[] request) throws IOException {
            String path = url.encodedPath();
            if ("/transactions".equals(path)) {
                return new HttpResponse(
                        201,
                        ("{\"transactionId\":\"" + TRANSACTION_ID + "\"}")
                                .getBytes(StandardCharsets.UTF_8));
            }
            if (path.endsWith("/commit") && failCommit) {
                throw new IOException("response lost");
            }
            return new HttpResponse(200, new byte[0]);
        }
    }
}
