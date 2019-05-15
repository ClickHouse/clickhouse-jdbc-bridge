package ru.yandex.clickhouse.jdbcbridge.servlet;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.StringUtil;
import ru.yandex.clickhouse.jdbcbridge.db.clickhouse.ClickHouseConverter;
import ru.yandex.clickhouse.jdbcbridge.db.jdbc.BridgeConnectionManager;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This servlet infer the schema of given table, and writes it back
 * Created by krash on 21.09.18.
 */
@Data
@Slf4j
public class ColumnsInfoServlet extends HttpServlet {

    private final BridgeConnectionManager manager;
    private final ClickHouseConverter converter;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try (Connection connection = manager.get(req.getParameter("connection_string")); Statement sth = connection.createStatement()) {
            String schema = req.getParameter("schema");
            String table = req.getParameter("table");

            String quote = connection.getMetaData().getIdentifierQuoteString();
            String schemaAndTable = Stream.of(schema, table)
                    .filter(s -> !StringUtil.isBlank(s))
                    .map(s -> quote + s + quote)
                    .collect(Collectors.joining("."));

            String queryRewrite = "SELECT * FROM " + schemaAndTable + " WHERE 1 = 0";
            log.info("Inferring schema by query {}", queryRewrite);

            ResultSet resultset = sth.executeQuery(queryRewrite);
            String ddl = converter.getColumnsDDL(resultset.getMetaData());
            resp.setContentType("application/octet-stream");
            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(resp.getOutputStream(), null, new ClickHouseProperties());
            stream.writeString(ddl);
        } catch (Exception err) {
            log.error(err.getMessage(), err);
            resp.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500, err.getMessage());
        }
    }
}
