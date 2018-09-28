package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This test performs creation of table into several DBMS, and performs attempt to
 * load the data from them
 * If path to clickhouse-local has been specified, it will try to validate serialized result
 * via it's invocation
 */
@RunWith(Parameterized.class)
@Slf4j
public class ClickHouseConverterIntegrationTest {

    private static final String DLL_CREATE_TABLE = "CREATE ${temporary} TABLE ${i}${table}${i} (${i}id${i} INTEGER, PRIMARY KEY (${i}id${i}))";
    private static final String DLL_ADD_COLUMN = "ALTER TABLE ${i}${table}${i} ADD COLUMN ${i}${name}${i} ${type} ${nullability}";
    private static final String DML_INSERT = "INSERT INTO ${i}${table}${i} (${i}id${i}, ${fields}) VALUES (100500, ${values})";
    private final static Map<String, Collection<String>> DB_UNSUPPORTED_TYPES = new HashMap<>();
    // If test is started with providing values for this particular connections,
    // then integration over this databases would be performed
    private static String[] POSSIBLE_DRIVER_PROPERTIES = new String[]{
            // Example: "jdbc:mysql://localhost:3306/test?user=root&password=root&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
            "datasource.mysql",
            // Example: "jdbc:postgresql://localhost:5432/test?user=root&password=root"
            "datasource.postgresql",
            "datasource.sqlite",
    };
    private static Collection<Object[]> DATA_TYPES = new ArrayList<>();

    static {
        DB_UNSUPPORTED_TYPES.put("datasource.postgresql", Arrays.asList("TINYINT", "DOUBLE"));
        DB_UNSUPPORTED_TYPES.put("datasource.sqlite", Arrays.asList("BOOLEAN", "TIME", "DATE"));
    }

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
        DATA_TYPES.add(new Object[]{"VARCHAR", "'VARCHAR_VALUE'", 64});
    }

    private final String uri;
    private final String tableName;
    private final Collection<String> unsupportedTypes;
    private String quote;
    private Connection conn;


    public ClickHouseConverterIntegrationTest(String uri, Collection<String> unsupported) {
        this.uri = uri;
        tableName = UUID.randomUUID().toString();
        this.unsupportedTypes = unsupported;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> provider() {
        List<Object[]> retval = new ArrayList<>();

        retval.add(new Object[]{"jdbc:h2:mem:test_database;MODE=MySQL", Collections.emptySet()});
        retval.add(new Object[]{"jdbc:sqlite::memory:", DB_UNSUPPORTED_TYPES.getOrDefault("datasource.sqlite", Collections.emptyList())});

        for (String property : POSSIBLE_DRIVER_PROPERTIES) {
            String databaseUri = System.getProperty(property, null);
            if (null != databaseUri) {
                Collection<String> unsupported = DB_UNSUPPORTED_TYPES.get(property);
                unsupported = null == unsupported ? Collections.emptyList() : unsupported;
                retval.add(new Object[]{databaseUri, unsupported});
            }
        }

        return retval;
    }

    @Before
    public void setUp() throws Exception {
        conn = DriverManager.getConnection(uri);
        quote = conn.getMetaData().getIdentifierQuoteString();

        Properties formatProps = new Properties();
        formatProps.setProperty("temporary", uri.startsWith("jdbc:h2:") ? "" : "TEMPORARY");
        formatProps.setProperty("i", conn.getMetaData().getIdentifierQuoteString());
        formatProps.setProperty("table", tableName);
        String ddl = StrSubstitutor.replace(DLL_CREATE_TABLE, formatProps);
        conn.createStatement().execute(ddl);

        List<String> fieldList = new ArrayList<>();
        List<String> valueList = new ArrayList<>();

        for (Object[] fieldAndValue : DATA_TYPES) {
            String type = fieldAndValue[0].toString();

            if (unsupportedTypes.contains(type)) {
                continue;
            }

            String name = type.toLowerCase();
            String value = fieldAndValue[1].toString();

            if (fieldAndValue.length == 3) {
                type = type + "(" + fieldAndValue[2] + ")";
            }

            formatProps.setProperty("name", name);
            formatProps.setProperty("type", type);
            formatProps.setProperty("value", value);
            formatProps.setProperty("nullability", "NOT NULL DEFAULT " + value);

            String addColumn = StrSubstitutor.replace(DLL_ADD_COLUMN, formatProps);
            conn.createStatement().execute(addColumn);

            // insert into this field later
            fieldList.add(quote + name + quote);
            valueList.add(value);

            // nullable
            formatProps.setProperty("name", name + "_nullable");
            formatProps.setProperty("nullability", "NULL DEFAULT NULL");
            addColumn = StrSubstitutor.replace(DLL_ADD_COLUMN, formatProps);
            conn.createStatement().execute(addColumn);
        }

        formatProps.setProperty("fields", String.join(", ", fieldList));
        formatProps.setProperty("values", String.join(", ", valueList));
        String insert = StrSubstitutor.replace(DML_INSERT, formatProps);
        conn.createStatement().execute(insert);

    }

    @After
    public void tearDown() throws Exception {
        if (null != conn && !conn.isClosed()) {
            conn.close();
        }
    }

    /**
     * Test that for given table all the fields can be converted to ClickHouse
     * data structures
     */
//    @Test
    public void testInferSchema() throws Exception {
        try (ResultSet resultset = conn.createStatement()
                .executeQuery("SELECT * FROM " + quote + tableName + quote + " WHERE 0 = 1")) {
            new ClickHouseConverter().getColumnsDDL(resultset.getMetaData());
        }

    }

    @Test
    public void testSerialize() throws Exception {
        try (ResultSet resultset = conn.createStatement()
                .executeQuery("SELECT * FROM " + quote + tableName + quote);
             ResultSet resultsetCopy = conn.createStatement()
                     .executeQuery("SELECT * FROM " + quote + tableName + quote)) {

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
                            ClickHouseConverter.getCLIStructure(resultsetCopy.getMetaData()),
                            "--input-format=RowBinary",
                            "--format=Vertical",
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
