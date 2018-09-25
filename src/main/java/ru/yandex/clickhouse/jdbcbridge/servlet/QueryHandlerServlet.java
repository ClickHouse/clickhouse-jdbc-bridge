package ru.yandex.clickhouse.jdbcbridge.servlet;

import com.google.common.util.concurrent.Runnables;
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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Time;
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
            try (Connection connection = DriverManager.getConnection(req.getParameter("connection_string")); Statement sth = connection.createStatement()) {
                ResultSet resultset = sth.executeQuery(query);
                ResultSetMetaData meta = resultset.getMetaData();
                resp.setContentType("application/octet-stream");
                while (resultset.next()) {
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        final boolean nullable = ResultSetMetaData.columnNullable == meta.isNullable(i);

                        Runnable cb = nullable ? () -> {
                            try {
                                stream.writeByte((byte) 0);
                            } catch (Exception err) {
                                throw new RuntimeException(err);
                            }

                        } : Runnables.doNothing();

                        switch (meta.getColumnType(i)) {
                            case Types.INTEGER:
                                cb.run();
                                stream.writeInt32(resultset.getInt(i));
                                break;
                            case Types.SMALLINT:
                                cb.run();
                                stream.writeInt16(resultset.getInt(i));
                                break;
                            case Types.FLOAT:
                            case Types.REAL:
                                cb.run();
                                stream.writeFloat32(resultset.getFloat(i));
                                break;
                            case Types.DOUBLE:
                                cb.run();
                                stream.writeFloat64(resultset.getDouble(i));
                                break;
                            case Types.TIMESTAMP:
                            case Types.TIME:
                                final Time time = resultset.getTime(i);
                                if (null == time) {
                                    stream.writeByte((byte) 1);
                                } else {
                                    cb.run();
                                    stream.writeDateTime(time);
                                }
                                break;
                            case Types.DATE:
                                final Date date = resultset.getDate(i);
                                if (null == date) {
                                    stream.writeByte((byte) 1);
                                } else {
                                    cb.run();
                                    stream.writeDate(date);
                                }
                                break;
                            default:
                                final String string = resultset.getString(i);
                                if (null == string) {
                                    stream.writeByte((byte) 1);
                                } else {
                                    cb.run();
                                    stream.writeString(string);
                                }

                                break;
                        }
                    }
                }
            }
        } catch (Exception err) {
            err.printStackTrace();
            resp.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500, err.getMessage());
//            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
//            resp.getWriter().write(err.getMessage());
        }
    }


}
