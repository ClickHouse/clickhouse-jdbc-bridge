package ru.yandex.clickhouse.jdbcbridge.servlet;

import lombok.SneakyThrows;
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
import java.util.function.Function;

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

                        Function<ResultSet, Boolean> markedAsNull = resultSet -> false;

                        if (nullable) {
                            markedAsNull = new Function<ResultSet, Boolean>() {
                                @Override
                                @SneakyThrows
                                public Boolean apply(ResultSet resultSet) {

                                    boolean retval = resultSet.wasNull();
                                    stream.writeByte((byte) (retval ? 1 : 0));

                                    return retval;
                                }
                            };
                        }

                        switch (meta.getColumnType(i)) {
                            case Types.INTEGER:
                                int value1 = resultset.getInt(i);
                                if (!markedAsNull.apply(resultset)) {
                                    stream.writeInt32(value1);
                                }
                                break;
                            case Types.SMALLINT:
                                int value2 = resultset.getInt(i);
                                if (!markedAsNull.apply(resultset)) {
                                    stream.writeInt16(value2);
                                }
                                break;
                            case Types.FLOAT:
                            case Types.REAL:
                                float value3 = resultset.getFloat(i);
                                if (!markedAsNull.apply(resultset)) {
                                    stream.writeFloat32(value3);
                                }
                                break;
                            case Types.DOUBLE:
                                double value4 = resultset.getDouble(i);
                                if (!markedAsNull.apply(resultset)) {
                                    stream.writeFloat64(value4);
                                }
                                break;
                            case Types.TIMESTAMP:
                            case Types.TIME:
                                final Time time = resultset.getTime(i);
                                if (!markedAsNull.apply(resultset) && null != time) {
                                    stream.writeDateTime(time);
                                }
                                break;
                            case Types.DATE:
                                final Date date = resultset.getDate(i);
                                if (!markedAsNull.apply(resultset) && null != date) {
                                    stream.writeDate(date);
                                }
                                break;
                            default:
                                final String string = resultset.getString(i);
                                if (!markedAsNull.apply(resultset) && null != string) {
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
