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

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private JsonUtil() {}

    private static class Holder {
        private static final ObjectMapper INSTANCE = initMapper();
    }

    public static ObjectMapper getInstance() {
        return Holder.INSTANCE;
    }

    private static ObjectMapper initMapper() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper;
    }
}
