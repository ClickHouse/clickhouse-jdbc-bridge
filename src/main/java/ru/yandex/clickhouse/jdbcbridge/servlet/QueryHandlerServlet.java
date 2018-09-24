package ru.yandex.clickhouse.jdbcbridge.servlet;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.StringUtil;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.guava.StreamUtils;

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
public class QueryHandlerServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String query = req.getParameter("query");
        try {
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
            System.out.println(query);

            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(resp.getOutputStream(), null, new ClickHouseProperties());
            try (Connection connection = DriverManager.getConnection("jdbc:mariadb://dbm1.d3:3306/?user=developer&password=developerovich"); Statement sth = connection.createStatement()) {
                ResultSet resultset = sth.executeQuery(query);
                ResultSetMetaData meta = resultset.getMetaData();
                resp.setContentType("application/octet-stream");
                while (resultset.next()) {
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        switch (meta.getColumnType(i)) {
                            case Types.INTEGER:
                                stream.writeInt32(resultset.getInt(i));
                            break;
                            case Types.SMALLINT:
                                stream.writeInt16(resultset.getInt(i));
                            break;
                            case Types.FLOAT:
                                stream.writeFloat32(resultset.getFloat(i));
                            break;
                            case Types.REAL:
                                stream.writeFloat32(resultset.getFloat(i));
                            break;
                            case Types.DOUBLE:
                                stream.writeFloat64(resultset.getDouble(i));
                            break;
                            case Types.TIMESTAMP:
                            case Types.TIME:
                                stream.writeDateTime(resultset.getTime(i));
                            break;
                            case Types.DATE:
                                stream.writeDate(resultset.getDate(i));
                            break;
                            default:
                                stream.writeString(resultset.getString(i));
                            break;
                        }
                    }
                }
            }
        } catch (Exception err) {
            resp.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500, err.getMessage());
//            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
//            resp.getWriter().write(err.getMessage());
        }
    }


}
