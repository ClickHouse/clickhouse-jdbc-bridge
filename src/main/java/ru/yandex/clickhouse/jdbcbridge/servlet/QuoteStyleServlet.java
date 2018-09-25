package ru.yandex.clickhouse.jdbcbridge.servlet;

import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by krash on 21.09.18.
 */
public class QuoteStyleServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try (Connection connection = DriverManager.getConnection(req.getParameter("connection_string"))) {
            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(resp.getOutputStream(), null, new ClickHouseProperties());
            final String identifierQuoteString = connection.getMetaData().getIdentifierQuoteString();
            resp.setContentType("application/octet-stream");
            stream.writeString(identifierQuoteString);
        } catch (Exception err) {
            throw new IOException(err);
        }
    }
}
