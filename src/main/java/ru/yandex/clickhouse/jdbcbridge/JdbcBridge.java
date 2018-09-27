package ru.yandex.clickhouse.jdbcbridge;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import ru.yandex.clickhouse.jdbcbridge.db.clickhouse.ClickHouseConverter;
import ru.yandex.clickhouse.jdbcbridge.db.jdbc.BridgeConnectionManager;
import ru.yandex.clickhouse.jdbcbridge.db.jdbc.JdbcDriverLoader;
import ru.yandex.clickhouse.jdbcbridge.servlet.ColumnsInfoServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.PingHandlerServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.QueryHandlerServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.QuoteStyleServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.RequestLogger;
import ru.yandex.clickhouse.util.apache.StringUtils;

import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.EnumSet;

/**
 * Created by krash on 21.09.18.
 */
@Slf4j
public class JdbcBridge implements Runnable {

    private static int DEFAULT_PORT = 9019;
    private static String DEFAULT_HOST = "localhost";

    private Arguments config;

    private JdbcBridge(String... argv) {
        try {
            config = parseArguments(argv);
        } catch (Throwable err) {
            log.error("Failed to parse config: {}", err.getMessage());
            System.exit(1);
        }
    }

    public static void main(String... argv) throws Exception {
        try {
            new JdbcBridge(argv).run();
        } catch (Exception err) {
            log.error("Stop JDBC bridge with error: {}", err.getMessage());
        }
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
            System.err.println("Error parsing incoming config: " + err.getMessage());
            System.exit(1);
        }
        return args;
    }

    @Override
    @SneakyThrows
    public void run() {
        log.info("Starting jdbc-bridge");

        JdbcDriverLoader.load(config.getDriverPath());

        BridgeConnectionManager manager = new BridgeConnectionManager();
        if (null != config.getConnectionFile()) {
            manager.load(config.getConnectionFile());
        }

        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(new ServletHolder(new QueryHandlerServlet(manager)), "/");
        handler.addServletWithMapping(new ServletHolder(new ColumnsInfoServlet(manager, new ClickHouseConverter())), "/columns_info");
        handler.addServletWithMapping(new ServletHolder(new QuoteStyleServlet(manager)), "/quote_style");
        handler.addServletWithMapping(new ServletHolder(new PingHandlerServlet()), "/ping");
        handler.addFilterWithMapping(RequestLogger.class, "/*", EnumSet.of(DispatcherType.REQUEST));

        InetSocketAddress address = new InetSocketAddress(config.getListenHost(), config.getHttpPort());
        log.info("Will bind to {}", address);

        // this tricks are don in order to get good thread name in logs :(
        QueuedThreadPool pool = new QueuedThreadPool(1024, 10); // @todo make configurable?
        pool.setName("HTTP Handler");
        Server jettyServer = new Server(pool);
        ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(address.getHostName());
        connector.setPort(address.getPort());
        jettyServer.setConnectors(new Connector[]{connector});

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

        @Parameter(names = "--connection-file", description = "File, containing specifications for connections")
        String connectionFile = null;

        @Parameter(names = "--help", help = true, description = "Show help message")
        boolean help = false;

        public Path getDriverPath() {
            return StringUtils.isBlank(driverPath) ? null : Paths.get(driverPath);
        }

        public File getConnectionFile() {
            return null == connectionFile ? null : new File(connectionFile);
        }
    }
}
