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

import com.alibaba.neug.driver.ResultSet;
import com.alibaba.neug.driver.Results;
import com.alibaba.neug.driver.internal.InternalResultSet;

/**
 * Utility class for parsing database server responses.
 *
 * <p>This class converts Protocol Buffers response bytes into ResultSet objects that can be used to
 * access query results.
 */
public class ResponseParser {

    /**
     * Parses a response byte array into a ResultSet.
     *
     * @param response the response bytes from the database server
     * @return a ResultSet containing the query results
     * @throws RuntimeException if the response cannot be parsed
     */
    public static ResultSet parse(byte[] response) {
        try {
            Results.QueryResponse queryResponse = Results.QueryResponse.parseFrom(response);
            return new InternalResultSet(queryResponse);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse response", e);
        }
    }
}
