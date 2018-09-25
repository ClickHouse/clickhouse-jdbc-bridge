package ru.yandex.clickhouse.jdbcbridge.db;

import org.eclipse.jetty.util.StringUtil;

import java.net.URI;
import java.sql.Connection;

/**
 * Created by krash on 25.09.18.
 */
public class BridgeConnectionManager {

    public Connection get(String uri) {
        if (StringUtil.isBlank(uri)) {
            throw new IllegalArgumentException("Empty connection string given");
        }
        URI uriObject = URI.create(uri);
        String schema;
        switch (schema = uriObject.getScheme()) {
            // an alias for pre-configured datasources
            case "alias":
                return null;

            // create datasource for given URI, and retrieve connection from there
            default:
                if (!schema.startsWith("jdbc:")) {
                    schema = "jdbc:" + schema;
                }
                return null;
        }
    }

}
