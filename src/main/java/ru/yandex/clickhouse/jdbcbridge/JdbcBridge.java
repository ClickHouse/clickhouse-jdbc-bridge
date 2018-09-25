package ru.yandex.clickhouse.jdbcbridge;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import ru.yandex.clickhouse.jdbcbridge.servlet.ColumnsInfoServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.PingHandlerServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.QueryHandlerServlet;
import ru.yandex.clickhouse.jdbcbridge.servlet.QuoteStyleServlet;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.CodeSource;

/**
 * Created by krash on 21.09.18.
 */
@Slf4j
public class JdbcBridge implements Runnable {

    private static int DEFAULT_PORT = 9019;
    private static String DEFAULT_HOST = "localhost";
    private static int HTTP_TIMEOUT = 1800;

    private Arguments arguments;

    private JdbcBridge(String...argv)
    {
        try {
            arguments = parseArguments(argv);
        } catch (Throwable err) {
            log.error("Failed to parse arguments: {}", err.getMessage());
            System.exit(1);
        }
    }

    @SneakyThrows
    private void runInternal()
    {
        log.info("Starting jdbc-bridge");
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
        }
        finally {
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

        @Parameter(names = "--help", help = true, description = "Show help message")
        boolean help = false;
    }
}
