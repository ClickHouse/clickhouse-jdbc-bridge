package ru.yandex.clickhouse.jdbcbridge;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.reflections.Reflections;
import ru.yandex.clickhouse.jdbcbridge.servlet.ColumnsInfoServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.PingHandlerServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.QueryHandlerServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.QuoteStyleServlet;
import ru.yandex.clickhouse.jdbcbridge.util.ChildFirstURLClassLoader;
import ru.yandex.clickhouse.jdbcbridge.util.MutableURLClassLoader;
import ru.yandex.clickhouse.util.apache.StringUtils;
import sun.misc.Service;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.CodeSource;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;

/**
 * Created by krash on 21.09.18.
 */
@Slf4j
public class JdbcBridge implements Runnable {

    private static int DEFAULT_PORT = 9019;
    private static String DEFAULT_HOST = "localhost";
    private static int HTTP_TIMEOUT = 1800;

    private Arguments arguments;

    private JdbcBridge(String... argv) {
        try {
            arguments = parseArguments(argv);
        } catch (Throwable err) {
            log.error("Failed to parse arguments: {}", err.getMessage());
            System.exit(1);
        }
    }

    /**
     * Append JARs from external directory to class path
     */
    @SneakyThrows
    private void loadDrivers(File sourceDir) {
        if (!sourceDir.exists()) {
            throw new IllegalArgumentException("Directory with drivers '" + sourceDir + "' does not exists");
        }
        File[] driverList = sourceDir.listFiles(file -> file.isFile() && file.getName().endsWith(".jar"));
        if (null == driverList) {
            driverList = new File[0];
        }
        log.info("Found {} JAR files in driver dir {}", driverList.length, sourceDir);

        MutableURLClassLoader loader = new MutableURLClassLoader(Thread.currentThread().getContextClassLoader());
//        MutableURLClassLoader loader = new ChildFirstURLClassLoader(Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);

        Arrays.stream(driverList).forEach(new Consumer<File>() {
            @Override
            @SneakyThrows
            public void accept(File file) {
                log.info("Registering external driver {}", file);
                loader.addURL(file.toURI().toURL());
            }
        });


        log.info("Loading driver list");
        Iterator<Driver> ps = Service.providers(Driver.class);
        log.info("List loaded");
        ps.forEachRemaining(new Consumer<Driver>() {
            @Override
            @SneakyThrows
            public void accept(Driver driver) {
                log.info("Found driver " + driver.getClass().getName());
                DriverManager.registerDriver(new DriverShim(driver));
            }
        });

        log.info("Final size is: {}", Iterators.size(Iterators.forEnumeration(DriverManager.getDrivers())));
        Enumeration<Driver> a = DriverManager.getDrivers();
        while (a.hasMoreElements()) {
            Driver driver = a.nextElement();
            String name = driver.getClass().getName();
            if (driver instanceof DriverShim) {
                name = ((DriverShim) driver).getWrappedClassName();
            }
            System.out.println(name + " " + driver.acceptsURL("jdbc:mysql://localhost/test"));
        }
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test");
        int r = 10;
    }

    @SneakyThrows
    private void runInternal() {
        log.info("Starting jdbc-bridge");

        if (!StringUtils.isBlank(arguments.getDriverPath())) {
            loadDrivers(new File(arguments.getDriverPath()));
        }

        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(new ServletHolder(new QueryHandlerServlet()), "/");
        handler.addServletWithMapping(new ServletHolder(new PingHandlerServlet()), "/ping");
        handler.addServletWithMapping(new ServletHolder(new ColumnsInfoServlet()), "/columns_info");
        handler.addServletWithMapping(new ServletHolder(new QuoteStyleServlet()), "/quote_style");

        InetSocketAddress address = new InetSocketAddress(arguments.getListenHost(), arguments.getHttpPort());
        log.info("Will bind to {}", address);
        Server jettyServer = new Server(address);
        jettyServer.setHandler(handler);
        jettyServer.setErrorHandler(new ErrorHandler() {
            @Override
            protected void handleErrorPage(HttpServletRequest request, Writer writer, int code, String message) throws IOException {
                writer.write(message);
            }
        });

        try {
            log.info("Starting server");
            jettyServer.start();
            log.info("Server is ready to accept connections");
            jettyServer.join();
        } finally {
            jettyServer.destroy();
        }
    }


    @Override
    public void run() {

        try {
            runInternal();
        } catch (Throwable err) {
            log.error("Fatal error, exiting", err);
            System.exit(1);
        } finally {
            log.info("Finish jdbc-bridge");
        }
    }

    public static void main(String... argv) throws Exception {
        new JdbcBridge(argv).run();
    }

    private static Arguments parseArguments(String... argv) {
        Arguments args = new Arguments();
        try {
            final JCommander build = JCommander.newBuilder()
                    .addObject(args)
                    .build();
            build.parse(argv);

            if (args.isHelp()) {

                // try to create full path to binary in help
                CodeSource src = JdbcBridge.class.getProtectionDomain().getCodeSource();
                if (src != null && src.getLocation().toString().endsWith(".jar")) {
                    URL jar = src.getLocation();
                    String jvmPath = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java";
                    build.setProgramName(jvmPath + " -jar " + jar.getPath());
                    build.setColumnSize(200);
                }

                build.usage();
                System.exit(0);
            }
        } catch (Exception err) {
            System.err.println("Error parsing incoming arguments: " + err.getMessage());
            System.exit(1);
        }
        return args;
    }

    public static class DriverShim implements Driver {
        private Driver driver;

        DriverShim(Driver d) {
            this.driver = d;
        }

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

    @Data
    public static class Arguments {
        @Parameter(names = "--http-port", description = "Port to listen on")
        int httpPort = DEFAULT_PORT;

        @Parameter(names = "--listen-host", description = "Host to listen on")
        String listenHost = DEFAULT_HOST;

        @Parameter(names = "--http-timeout", description = "A timeout for dealing with database")
        int httpTimeout = 1800;

        @Parameter(names = "--log-path", description = "Where to write logs")
        String logPath = null;

        @Parameter(names = "--err-log-path", description = "Where to redirect STDERR")
        String errorLogPath = null;

        @Parameter(names = "--log-level", description = "Log level")
        String logLevel = "DEBUG";

        @Parameter(names = "--driver-path", description = "Path to directory, containing JDBC drivers")
        String driverPath = null;

        @Parameter(names = "--help", help = true, description = "Show help message")
        boolean help = false;
    }
}
