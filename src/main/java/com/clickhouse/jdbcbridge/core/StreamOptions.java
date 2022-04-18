/**
 * Copyright 2019-2022, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clickhouse.jdbcbridge.core;

import io.vertx.core.MultiMap;

/**
 * This class defines options for streaming.
 * 
 * @since 2.0
 */
public class StreamOptions {
    private static final String PARAM_MAX_BLOCK_SIZE = "max_block_size";

    public static final int DEFAULT_BLOCK_SIZE = 65535;

    private final int maxBlockSize;

    public StreamOptions(MultiMap params) {
        int blockSize = DEFAULT_BLOCK_SIZE;
        try {
            blockSize = Integer.parseInt(params.get(PARAM_MAX_BLOCK_SIZE));
        } catch (Exception e) {
        }

        this.maxBlockSize = blockSize;
    }

    public int getMaxBlockSize() {
        return this.maxBlockSize;
    }
}