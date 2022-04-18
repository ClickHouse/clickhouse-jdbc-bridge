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
import java.util.TimeZone;

/**
 * Interface for reading tabular data and writing to response.
 * 
 * @since 2.0
 */
public interface DataTableReader {
    /**
     * Move cursor to next row. This should be called at least once before read
     * anything.
     * 
     * @return {@code true} if there's more rows to read; {@code false} otherwise
     */
    boolean nextRow() throws DataAccessException;

    /**
     * Check whether a cell is null or not.
     * 
     * @param row      zero-based row index
     * @param column   zero-based column index
     * @param metadata column metadata
     * @return {@code true} if the cell is null; {@code false} otherwise
     */
    boolean isNull(int row, int column, ColumnDefinition metadata) throws DataAccessException;

    /**
     * Read value from a cell and write into given {@link ByteBuffer}.
     * 
     * @param row      zero-based row index
     * @param column   zero-based column index
     * @param metadata column metadata
     * @param buffer   byte buffer
     */
    void read(int row, int column, ColumnDefinition metadata, ByteBuffer buffer) throws DataAccessException;

    /**
     * Stream tabular data to response.
     * 
     * @param dataSourceId   id of the datasource
     * @param requestColumns requested columns
     * @param customColumns  custom columns defined in datasource
     * @param resultColumns  result columns
     * @param defaultValues  default values defined in datasource
     * @param timezone       preferred timezone
     * @param params         query parameters
     * @param writer         response writer
     */
    default void process(String dataSourceId, ColumnDefinition[] requestColumns, ColumnDefinition[] customColumns,
            ColumnDefinition[] resultColumns, DefaultValues defaultValues, TimeZone timezone, QueryParameters params,
            ResponseWriter writer) throws DataAccessException {
        Objects.requireNonNull(dataSourceId);
        Objects.requireNonNull(requestColumns);
        Objects.requireNonNull(customColumns);
        Objects.requireNonNull(resultColumns);
        Objects.requireNonNull(defaultValues);
        Objects.requireNonNull(params);
        Objects.requireNonNull(writer);

        // Map<String, Integer> colName2Index = new HashMap<>();
        // build column indices: 0 -> Request column index; 1 -> ResultSet column index
        int length = requestColumns.length;
        int[][] colIndices = new int[length][2];

        for (int i = 0; i < length; i++) {
            boolean matched = false;
            ColumnDefinition col = requestColumns[i];
            String colName = col.getName();

            // let's check if it's a virtual column which does not exist in result first
            if (params.showDatasourceColumn() && TableDefinition.COLUMN_DATASOURCE.equals(colName)) {
                // data source column
                colIndices[i] = new int[] { i, -1 };
                matched = true;
                continue;
            } else if (params.showCustomColumns()) {
                // one of custom columns
                for (int k = 0; k < customColumns.length; k++) {
                    if (colName.equals(customColumns[k].getName())) {
                        colIndices[i] = new int[] { k, -2 };
                        matched = true;
                        break;
                    }
                }
            }

            // now result columns
            if (!matched) {
                for (int j = 0; j < resultColumns.length; j++) {
                    ColumnDefinition result = resultColumns[j];
                    if (colName.equals(result.getName())) {
                        // colIndices[i] = new int[] { i, j + 1 };
                        colIndices[i] = new int[] { i, j };
                        matched = true;
                        break;
                    }
                }
            }

            if (!matched) { // should not happen...
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < resultColumns.length; j++) {
                    sb.append(',').append('[').append(resultColumns[j].getName()).append(']');
                }
                if (sb.length() > 0) {
                    sb.deleteCharAt(0);
                }

                throw new IllegalArgumentException(
                        "Unknown column [" + colName + "]! Available columns are: " + sb.toString());
            }
        }

        // now let's read rows
        int rowCount = params.isMutation() ? 0 : this.skipRows(params);
        int batchSize = params.getBatchSize();
        if (batchSize <= 0) {
            batchSize = 1;
        }
        int estimatedBufferSize = length * 4 * batchSize;

        ByteBuffer buffer = ByteBuffer.newInstance(estimatedBufferSize, timezone);
        boolean skipped = rowCount > 0;
        while (skipped || nextRow()) {
            skipped = false;

            for (int i = 0; i < length; i++) {
                int[] indices = colIndices[i];

                int colIndex = indices[0]; // Request column index
                int index = indices[1]; // ResultSet column index

                if (index == -1) { // must be datasource column
                    buffer.writeNonNull().writeString(dataSourceId);
                    continue;
                } else if (index == -2) { // must be one of custom columns
                    customColumns[colIndex].writeValueTo(buffer);
                    continue;
                }

                ColumnDefinition column = requestColumns[colIndex];

                if (column.isNullable()) {
                    // FIXME what if it's large object(e.g. blob, clob etc.)?
                    if (isNull(rowCount, index, column)) {
                        if (params.nullAsDefault()) {
                            // column.writeValueTo(buffer);
                            buffer.writeNonNull().writeDefaultValue(column, defaultValues);
                        } else {
                            buffer.writeNull();
                        }
                        continue;
                    } else {
                        buffer.writeNonNull();
                    }
                }

                read(rowCount, index, column, buffer);
            }

            if (++rowCount % batchSize == 0) {
                writer.write(buffer);

                buffer = ByteBuffer.newInstance(estimatedBufferSize, timezone);
            }
        }

        if (rowCount % batchSize != 0) {
            writer.write(buffer);
        }
    }

    default int skipRows(QueryParameters parameters) {
        int rowCount = 0;

        if (parameters == null) {
            return rowCount;
        }

        int position = parameters.getPosition();
        // absolute position takes priority
        if (position != 0) {
            if (position < 0) {
                throw new IllegalArgumentException("Only positive position is supported!");
            }

            // position of the first row is 1
            for (int i = 0; i < position; i++) {
                if (nextRow()) {
                    rowCount++;
                    continue;
                } else {
                    throw new IllegalStateException(
                            "Not able to move cursor to row #" + position + "as we only got " + i);
                }
            }
        } else { // now skip rows as needed
            int offset = parameters.getOffset();

            if (offset < 0) {
                throw new IllegalArgumentException("Only positive offset is supported!");
            } else if (offset != 0) {
                int counter = offset;
                while (nextRow()) {
                    rowCount++;

                    if (--offset <= 0) {
                        break;
                    }
                }

                if (offset != 0) {
                    throw new IllegalStateException("Not able to move cursor to row #" + (counter + 1)
                            + " as we only got " + (counter - offset));
                }
            }
        }

        return rowCount;
    }
}
