package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import lombok.Data;

/**
 * Created by krash on 26.09.18.
 */
@Data
public class ExtractorConverter<T> {

    private final FieldValueExtractor<T> extractor;
    private final FieldValueSerializer<T> serializer;

}
