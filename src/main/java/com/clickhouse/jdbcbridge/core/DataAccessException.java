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

import java.util.Objects;

/**
 * Thrown by {@link DataTableReader} when a problem occurs.
 * 
 * @since 2.0
 */
public class DataAccessException extends RuntimeException {
    private static final long serialVersionUID = -37476939631473275L;

    static final String ERROR_BEGIN = "Failed to access [";
    static final String ERROR_END = "] due to: ";

    static final String UNKNOWN_ERROR = "unknown error";

    static String buildErrorMessage(String dataSourceId, String message, Throwable cause) {
        return new StringBuilder().append(ERROR_BEGIN).append(dataSourceId).append(ERROR_END).append(
                message != null && !message.isEmpty() ? message : (cause == null ? UNKNOWN_ERROR : cause.getMessage()))
                .toString();
    }

    /**
     * Constructor for {@code DataAccessException}.
     * 
     * @param dataSourceId datasource id
     * @param cause        root cause
     */
    public DataAccessException(String dataSourceId, Throwable cause) {
        this(dataSourceId, null, cause);
    }

    /**
     * Constructor for {@code DataAccessException}.
     * 
     * @param dataSourceId datasource id
     * @param message      detailed message
     * @param cause        root cause
     */
    public DataAccessException(String dataSourceId, String message, Throwable cause) {
        super(buildErrorMessage(Objects.requireNonNull(dataSourceId), message, cause), Objects.requireNonNull(cause));
    }
}
