package ru.yandex.clickhouse.jdbcbridge.servlet;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.StringUtil;
import ru.yandex.clickhouse.jdbcbridge.db.clickhouse.ClickHouseRowSerializer;
import ru.yandex.clickhouse.jdbcbridge.db.jdbc.BridgeConnectionManager;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * Created by krash on 21.09.18.
 */
@Data
@Slf4j
public class QueryHandlerServlet extends HttpServlet {

    private final BridgeConnectionManager manager;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {
            String query = req.getParameter("query");
            if (StringUtil.isBlank(query)) {
                // a hack for wrong input from CH
                String requestBody = StreamUtils.toString(req.getInputStream());
                String[] parts = requestBody.split("query=", 2);
                if (parts.length == 2) {
                    query = parts[1];
                }
            }

            if (StringUtil.isBlank(query)) {
                throw new IllegalArgumentException("Query is blank or empty");
            }

            try (Connection connection = manager.get(req.getParameter("connection_string")); Statement sth = connection.createStatement()) {
                ResultSet resultset = sth.executeQuery(query);
                ResultSetMetaData meta = resultset.getMetaData();

                ClickHouseRowSerializer serializer = ClickHouseRowSerializer.create(meta);
                ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(resp.getOutputStream(), null, new ClickHouseProperties());

                resp.setContentType("application/octet-stream");
                while (resultset.next()) {
                    serializer.serialize(resultset, stream);
                }
            }
        } catch (Exception err) {
            log.error(err.getMessage(), err);
            resp.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500, err.getMessage());
        }
    }


}
