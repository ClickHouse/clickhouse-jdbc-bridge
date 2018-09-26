package ru.yandex.clickhouse.jdbcbridge.db.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.util.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class provides possibility for getting connections via
 * full DSN, or by alias (alias://localhost)
 * Created by krash on 25.09.18.
 */
@Slf4j
public class BridgeConnectionManager {

    private static final String ALIAS_SCHEMA = "alias";
    private static final String ALIAS_CONFIG_PREFIX = "connection.";
    private static final String CLIENT_NAME = "clickhouse-jdbc-bridge";

    private final Map<String, String> aliasMap = new HashMap<>();

    public void load(File aliasSpecificationFile) throws IOException {

        log.info("Loading aliases from file {}", aliasSpecificationFile);
        Properties properties = new Properties();
        try (InputStream stream = new FileInputStream(aliasSpecificationFile)) {
            properties.load(stream);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (key.startsWith(ALIAS_CONFIG_PREFIX) && !value.isEmpty()) {
                key = key.substring(ALIAS_CONFIG_PREFIX.length());

                try {
                    // validate URI
                    URI uri = new URI(value);
                    // log only part
                    log.info("Registering alias '{}' for {}://{}****", key, uri.getScheme(), uri.getHost());

                    aliasMap.put(key, value);
                } catch (URISyntaxException err) {
                    throw new IllegalArgumentException("Failed to validate DSN for alias '" + key + "' - " + value + ", error: " + err.getMessage());
                }
            }
        }
    }

    public Connection get(String uri) throws SQLException {
        if (StringUtil.isBlank(uri)) {
            throw new IllegalArgumentException("Empty connection string given");
        }
        URI uriObject = URI.create(uri);
        if (ALIAS_SCHEMA.equals(uriObject.getScheme())) {
            uri = aliasMap.get(uriObject.getHost());
            if (null == uri) {
                throw new SQLException("Unknown connection alias '" + uriObject.getHost() + "' given");
            }
        }

        if (!uri.startsWith("jdbc:")) {
            uri = "jdbc:" + uri;
        }

        Connection conn = DriverManager.getConnection(uri);

        // to avoid transactions
        conn.setAutoCommit(true);
        // to determine origin of the query
        conn.setClientInfo("ClientUser", CLIENT_NAME);

        return conn;
    }
}
