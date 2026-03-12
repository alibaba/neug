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
package org.alibaba.neug.driver.internal;

import org.alibaba.neug.driver.Driver;
import org.alibaba.neug.driver.Session;
import org.alibaba.neug.driver.utils.AccessMode;
import org.alibaba.neug.driver.utils.Client;
import org.alibaba.neug.driver.utils.Config;

/**
 * Internal implementation of the {@link Driver} interface.
 *
 * <p>This class manages the lifecycle of database connections and provides session creation
 * capabilities. It uses an HTTP client to communicate with the NeuG database server.
 */
public class InternalDriver implements Driver {

    private static Client client = null;

    /**
     * Constructs a new InternalDriver with the specified URI and configuration.
     *
     * @param uri the URI of the database server
     * @param config the configuration for the driver
     */
    public InternalDriver(String uri, Config config) {
        client = new Client(uri, config);
    }

    @Override
    public Session session() {
        return new InternalSession(client);
    }

    @Override
    public void verifyConnectivity() {
        try (Session session = session()) {
            session.run("RETURN 1", null, AccessMode.READ);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }
}
