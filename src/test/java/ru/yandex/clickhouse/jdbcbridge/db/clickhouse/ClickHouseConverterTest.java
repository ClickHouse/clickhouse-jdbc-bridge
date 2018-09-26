package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import org.h2.Driver;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.fail;
import static org.junit.Assume.*;

public class ClickHouseConverterTest {

    private Connection conn;

    private static Collection<Object[]> DATA_TYPES = new ArrayList<>();

    static {
        DATA_TYPES.add(new Object[]{"INTEGER", 10});
        DATA_TYPES.add(new Object[]{"BOOLEAN", true});
        DATA_TYPES.add(new Object[]{"TINYINT", 20});
        DATA_TYPES.add(new Object[]{"SMALLINT", 30});
        DATA_TYPES.add(new Object[]{"BIGINT", 1_000_000L});
        DATA_TYPES.add(new Object[]{"FLOAT", 123.4567d});
        DATA_TYPES.add(new Object[]{"DOUBLE", 789.0123d});
        DATA_TYPES.add(new Object[]{"REAL", 100500.123d});
        DATA_TYPES.add(new Object[]{"TIME", "'10:20:30'"});
        DATA_TYPES.add(new Object[]{"DATE", "'2018-03-04'"});
        DATA_TYPES.add(new Object[]{"TIMESTAMP", "'2018-03-04 05:06:07'"});
        DATA_TYPES.add(new Object[]{"VARCHAR", "'VARCHAR_VALUE'"});
    }

    @Before
    public void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:mem:test_database;MODE=MySQL");

        String columnSpec = DATA_TYPES.stream().flatMap(new Function<Object[], Stream<String>>() {
            @Override
            public Stream<String> apply(Object[] objects) {

                String type = objects[0].toString();

                String fieldName = "field_" + type.toLowerCase();
                return Stream.of(fieldName + " " + type + " NOT NULL", fieldName + "_null " + type + " NULL DEFAULT NULL");
            }
        }).collect(Collectors.joining(",\n"));

        String ddl = "CREATE TABLE test (" + columnSpec + ")";
        conn.createStatement().execute(ddl);

        List<String> setSpecList = new ArrayList<>();
        for (Object[] spec : DATA_TYPES) {
            String baseName = "field_" + spec[0].toString().toLowerCase();
            setSpecList.add(baseName + " = " + spec[1].toString());
        }

        String insert = "INSERT INTO test SET " + String.join(",\n", setSpecList);
        conn.createStatement().execute(insert);
        conn.commit();


    }

    @After
    public void tearDown() throws Exception {
        if (null != conn && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    public void testInferSchema() throws Exception {
        try (ResultSet resultset = conn.createStatement()
                .executeQuery("SELECT * FROM test WHERE 0 = 1")) {
            new ClickHouseConverter().getColumnsDDL(resultset.getMetaData());
        }

    }

    @Test
    public void testSerialize() throws Exception {
        try (ResultSet resultset = conn.createStatement()
                .executeQuery("SELECT * FROM test")) {

            File tmp = File.createTempFile("pref", "suf");
            tmp.deleteOnExit();
            OutputStream outputStream = new FileOutputStream(tmp);

            ClickHouseRowSerializer ser = ClickHouseRowSerializer.create(resultset.getMetaData());
            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(outputStream, null, new ClickHouseProperties());

            while (resultset.next()) {
                ser.serialize(resultset, stream);
            }

            outputStream.flush();
            outputStream.close();

            // If a path to binary clickhouse-local has been specified, try to validate file content via invocation
            String clickHouseLocalBinaryPath = System.getProperty("clickhouse.local.bin", null);
            assumeTrue(clickHouseLocalBinaryPath != null);

            ProcessBuilder builder = new ProcessBuilder()
                    .command(clickHouseLocalBinaryPath,
                            "--structure",
                            ClickHouseConverter.getCLIStructure(resultset.getMetaData()),
                            "--input-format=RowBinary",
                            "--query",
                            "SELECT * FROM table",
                            "--file=" + tmp + ""
                    );

            builder.inheritIO();
            Process process = builder.start();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroy();
                fail("Failed to validate output via invocation of clickhouse-local");
            } else if (process.exitValue() != 0) {
                fail("clickhouse-local exited with non-zero code");
            }
        }
    }

}
