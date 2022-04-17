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

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;

/**
 * This class defines how we write data to http response.
 * 
 * @since 2.0
 */
public class ResponseWriter {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ResponseWriter.class);

    private final HttpServerResponse response;
    private final StreamOptions options;
    private final long timeout;

    private final long startTime;

    public ResponseWriter(HttpServerResponse response, StreamOptions options, int timeout) {
        this.response = response;
        this.options = options;
        this.timeout = timeout * 1000L;

        this.startTime = System.currentTimeMillis();

        this.response.setWriteQueueMaxSize(this.options.getMaxBlockSize());

        if (log.isDebugEnabled()) {
            log.debug("Start Time={}, Timeout={}, Max Block Size={}", this.startTime, this.timeout,
                    this.options.getMaxBlockSize());
        }
    }

    public StreamOptions getOptions() {
        return this.options;
    }

    public boolean isOpen() {
        return !this.response.closed() && !this.response.ended();
    }

    public void setDrainHanlder(Handler<Void> handler) {
        this.response.drainHandler(handler);
    }

    public void write(ByteBuffer buffer) {
        if (this.response.closed() || this.response.ended()) {
            if (buffer != null && buffer.length() > 0) {
                log.warn("Still have at least {} bytes in buffer", buffer.length());
            }

            throw new IllegalStateException("Response stream was closed");
        }

        if (this.timeout > 0 && ((System.currentTimeMillis() - this.startTime) > this.timeout)) {
            throw new IllegalStateException("Abort due to timeout");
        }

        this.response.write(buffer.unwrap());
    }
}
