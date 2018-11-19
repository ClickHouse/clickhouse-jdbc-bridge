package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.jooq.CreateTableAsStep;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.SQLDataType;
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
            // Example: "jdbc:oracle:thin:@localhost:1521:xe"
            "datasource.oracle"
    };
    private static Collection<ColumnSpec> DATA_TYPES = new ArrayList<>();

    static {
        DB_UNSUPPORTED_TYPES.put("datasource.postgresql", Arrays.asList("TINYINT", "DOUBLE", "NVARCHAR"));
        DB_UNSUPPORTED_TYPES.put("datasource.sqlite", Arrays.asList("BOOLEAN", "TIME", "DATE"));
    }

    static {
        // scalar int
        DATA_TYPES.add(new ColumnSpec("INTEGER", 10));
        DATA_TYPES.add(new ColumnSpec("TINYINT", 20));
        DATA_TYPES.add(new ColumnSpec("SMALLINT", 20));
        DATA_TYPES.add(new ColumnSpec("BIGINT", 1_000_000L));

        // floating point
        DATA_TYPES.add(new ColumnSpec("FLOAT", 123.4567f));
        DATA_TYPES.add(new ColumnSpec("DOUBLE", 789.0123d));
        DATA_TYPES.add(new ColumnSpec("REAL", 100500.123d));

        // time
        DATA_TYPES.add(new ColumnSpec("TIME", "'10:20:30'"));
        DATA_TYPES.add(new ColumnSpec("DATE", "'2018-03-04'"));
        DATA_TYPES.add(new ColumnSpec("TIMESTAMP", "'2018-03-04 05:06:07'"));

        // text
        DATA_TYPES.add(new ColumnSpec("CHAR", "'v'").setColumnName("char_1"));
        DATA_TYPES.add(new ColumnSpec("CHAR", "'val'", 5).setColumnName("char_5"));
        DATA_TYPES.add(new ColumnSpec("VARCHAR", "'VARCHAR_VALUE'", 64));
        DATA_TYPES.add(new ColumnSpec("NVARCHAR", "'NVARCHAR_VALUE'", 64));
        DATA_TYPES.add(new ColumnSpec("NCHAR", "'NVARCHAR_VALUE'", 64));

        DATA_TYPES.add(new ColumnSpec("BOOLEAN", true));
    }

    private final String uri;
    private final String tableName;
    private final Collection<String> unsupportedTypes;
    private String quote;
    private Connection conn;

    public ClickHouseConverterIntegrationTest(String uri, Collection<String> unsupported) {
        this.uri = uri;
        tableName = UUID.randomUUID().toString().substring(0,6);
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

        DefaultDSLContext ctx = new DefaultDSLContext(SQLDialect.MYSQL_5_7);
        CreateTableConstraintStep smth = DSL.createGlobalTemporaryTable(tableName).column("id", SQLDataType.INTEGER).constraint(DSL.constraint("id").primaryKey("id"));

        conn = DriverManager.getConnection(uri, new Properties());
        quote = conn.getMetaData().getIdentifierQuoteString();

        Properties formatProps = new Properties();
        formatProps.setProperty("temporary", uri.startsWith("jdbc:h2:") ? "" : "TEMPORARY");
        formatProps.setProperty("i", conn.getMetaData().getIdentifierQuoteString());
        formatProps.setProperty("table", tableName);
        String ddl = StrSubstitutor.replace(DLL_CREATE_TABLE, formatProps);
        System.out.println(ddl);
        conn.createStatement().execute(ddl);

        List<String> fieldList = new ArrayList<>();
        List<String> valueList = new ArrayList<>();

        for (ColumnSpec spec : DATA_TYPES) {
            String type = spec.getTypename();

            if (unsupportedTypes.contains(type)) {
                continue;
            }

            String name = spec.getColumnName();
            String value = spec.getValue().toString();

            if (spec.getFieldLength() > 0) {
                type = type + "(" + spec.getFieldLength() + ")";
            }

            formatProps.setProperty("name", name);
            formatProps.setProperty("type", type);
            formatProps.setProperty("value", value);
            formatProps.setProperty("nullability", "NOT NULL DEFAULT " + value);

            String addColumn = StrSubstitutor.replace(DLL_ADD_COLUMN, formatProps);
            System.out.println(addColumn);
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

    private static class ColumnSpec {
        @Getter
        private final String typename;
        @Getter
        private final Object value;
        @Getter
        private int fieldLength = 0;

        private String columnName;

        private ColumnSpec(String typename, Object value) {
            this.typename = typename;
            this.value = value;
        }

        private ColumnSpec(String typename, Object value, int fieldLength) {
            this(typename, value);
            this.fieldLength = fieldLength;
        }

        public String getColumnName() {
            if (null == columnName) {
                return typename.toLowerCase() + "_1";
            }
            return columnName;
        }

        public ColumnSpec setColumnName(String value) {
            this.columnName = value;
            return this;
        }
    }

}
