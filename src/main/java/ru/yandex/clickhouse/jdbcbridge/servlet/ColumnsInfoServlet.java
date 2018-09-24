package ru.yandex.clickhouse.jdbcbridge.servlet;

import org.eclipse.jetty.util.StringUtil;
import ru.yandex.clickhouse.ClickHouseUtil;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

/**
 * Created by krash on 21.09.18.
 */
public class ColumnsInfoServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try (Connection connection = DriverManager.getConnection("jdbc:mariadb://dbm1.d3:3306/?user=developer&password=developerovich"); Statement sth = connection.createStatement()) {
            String table = req.getParameter("table");
            String schema = req.getParameter("schema");

            String tableAndSchema = StringUtil.isBlank(schema) ? table : schema + "." + table;

            String queryRewrite = "SELECT * FROM " + tableAndSchema + " WHERE 1 = 0";
            ResultSet resultset = sth.executeQuery(queryRewrite);
            resp.setContentType("application/octet-stream");
            ResultSetMetaData meta = resultset.getMetaData();

            StringBuilder builder = new StringBuilder("columns format version: 1\n");
            builder.append(meta.getColumnCount());
            builder.append(" columns:\n");
            for (int i = 0; i < meta.getColumnCount(); i++) {
                builder.append(ClickHouseUtil.quoteIdentifier(meta.getColumnName(i + 1)));
                builder.append(" ");
                builder.append(extractClickHouseDataType(meta.getColumnType(i + 1)));
                builder.append('\n');
            }
            resp.setContentType("application/octet-stream");
            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(resp.getOutputStream(), null, new ClickHouseProperties());
            stream.writeString(builder.toString());
        } catch (Exception err) {
            throw new IOException(err);
        }
    }

    private String extractClickHouseDataType(int columnType) {

        switch (columnType) {
            case Types.INTEGER:
                return "Int32";
            case Types.SMALLINT:
                return "Int16";
            case Types.FLOAT:
                return "Float32";
            case Types.REAL:
                return "Float32";
            case Types.DOUBLE:
                return "Float64";
            case Types.TIMESTAMP:
            case Types.TIME:
                return "DateTime";
            case Types.DATE:
                return "Date";
            default:
                return "String";
        }
    }
}
