package ru.yandex.clickhouse.jdbcbridge.db.jdbc;

import lombok.Data;
import lombok.NonNull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A wrapper for external drivers
 * Needed, because external drivers are loaded via
 * custom classloader, and JVM DriverManager accepts only
 * one, loaded by system classloader
 * Created by krash on 26.09.18.
 */
@Data
public class DriverWrapper implements Driver {

    @NonNull
    private final Driver driver;

    @Override
    public Connection connect(String s, Properties properties) throws SQLException {
        return driver.connect(s, properties);
    }

    @Override
    public boolean acceptsURL(String u) throws SQLException {
        return this.driver.acceptsURL(u);
    }

    @Override
    public int getMajorVersion() {
        return this.driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return this.driver.getMinorVersion();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
        return this.driver.getPropertyInfo(u, p);
    }

    @Override
    public boolean jdbcCompliant() {
        return this.driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }

    public String getWrappedClassName() {
        return driver.getClass().getName();
    }
}
