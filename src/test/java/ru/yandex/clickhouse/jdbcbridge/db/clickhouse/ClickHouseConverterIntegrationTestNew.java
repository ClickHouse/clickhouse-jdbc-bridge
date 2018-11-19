package ru.yandex.clickhouse.jdbcbridge.db.clickhouse;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.using;
import static org.junit.Assume.assumeNotNull;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.AlterTableFinalStep;
import org.jooq.CreateTableAsStep;
import org.jooq.CreateTableColumnStep;
import org.jooq.CreateTableConstraintStep;
import org.jooq.CreateTableFinalStep;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 *
 */
@Slf4j
public abstract class ClickHouseConverterIntegrationTestNew {

    private static final Collection<Field<?>> FIELDS = new ArrayList<>();

    static {
        FIELDS.add(field("id", SQLDataType.INTEGER));
        FIELDS.add(field("tinyint_", SQLDataType.TINYINT.defaultValue(Byte.valueOf("4"))));
        FIELDS.add(field("smallint_", SQLDataType.SMALLINT.defaultValue((short) 87)));
        FIELDS.add(field("bigint_", SQLDataType.BIGINT.defaultValue(100500L)));

        FIELDS.add(field("real_", SQLDataType.REAL.defaultValue(111.111f)));
        FIELDS.add(field("float_", SQLDataType.FLOAT.defaultValue(222.222d)));
        FIELDS.add(field("double_", SQLDataType.DOUBLE.defaultValue(333.333d)));

        FIELDS.add(field("time_", SQLDataType.TIME.defaultValue(new Time(3600))));
        FIELDS.add(field("date_", SQLDataType.DATE.defaultValue(new Date(7200))));
        final Timestamp timestamp = new Timestamp(100);
        timestamp.setNanos(0);
//        FIELDS.add(field("datetime_", SQLDataType.TIMESTAMP(2).defaultValue(timestamp)));

        FIELDS.add(field("char_", SQLDataType.CHAR.defaultValue("y")));
        FIELDS.add(field("char_2_", SQLDataType.CHAR(2).defaultValue("x")));
        FIELDS.add(field("varchar_", SQLDataType.VARCHAR(1).defaultValue("v")));
        FIELDS.add(field("varchar_5_", SQLDataType.VARCHAR(5).defaultValue("varc")));
        FIELDS.add(field("nchar_", SQLDataType.NCHAR.defaultValue("n")));
        FIELDS.add(field("nchar_5_", SQLDataType.NCHAR(5).defaultValue("nchar")));
        FIELDS.add(field("nvchar_", SQLDataType.NVARCHAR(1).defaultValue("z")));
        FIELDS.add(field("nvchar_6_", SQLDataType.NVARCHAR(6).defaultValue("nvchar")));

        FIELDS.add(field("bool_", SQLDataType.BOOLEAN.defaultValue(true)));
        FIELDS.add(field("bit_", SQLDataType.BIT.defaultValue(false)));

    }

    private Connection conn;

    private DSLContext ctx;

    protected boolean isTypeSupported(DataType<?> type)
    {
        return true;
    }

    /**
     * Implementations should provide connection params here
     */
    protected abstract ConnectionSpec getConnectionSpec();

    /**
     * Returns dialect for database interaction
     */
    protected abstract SQLDialect getDialect();

    protected final String getTableName() {
        return "clickhouse_jdbc_bridge";
    }

    @Before
    public void setUp() throws Exception {
        ConnectionSpec spec = getConnectionSpec();
        assumeNotNull(spec);

        conn = DriverManager.getConnection(spec.getConnectionString(), spec.getProperties());

        ctx = using(conn, getDialect());

        try {
            ctx.dropTable(getTableName()).execute();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

        CreateTableConstraintStep tbl = ctx.createGlobalTemporaryTable(getTableName())
                .column("id", SQLDataType.INTEGER.nullable(false))
                .constraint(
                        constraint().primaryKey("id")
                );
        createTable(tbl);


        for (Field<?> field : FIELDS) {
            if (field.getName().equals("id") || !isTypeSupported(field.getDataType())) {
                continue;
            }
            addField(ctx, conn, field);
        }

        InsertValuesStepN<Record> insert = ctx.insertInto(DSL.table(getTableName())).columns(FIELDS);
        log.info(insert.getSQL());
        insert.execute();
    }

    protected void addField(DSLContext ctx, Connection conn, Field<?> field) throws Exception {
        AlterTableFinalStep alter = ctx.alterTable(getTableName()).add(field);
        final String sql = alter.getSQL(ParamType.INLINED);
        log.info(sql);
        alter.execute();
    }

    protected void createTable(CreateTableFinalStep step) throws Exception {
        log.info(step.getSQL());
        step.execute();
    }

    @After
    public void tearDown() throws Exception {
        if (null != conn && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    public void test() {

    }

    /**
     * Test that for given table all the fields can be converted to ClickHouse
     * data structures
     */
    @Test
    public void testInferSchema() throws Exception {
        String ident = conn.getMetaData().getIdentifierQuoteString();
        try (ResultSet resultset = conn.createStatement().executeQuery("SELECT * FROM " + ident + getTableName() + ident + " WHERE 1 = 0")) {
            new ClickHouseConverter().getColumnsDDL(resultset.getMetaData());
        }
    }

    @Data
    protected static class ConnectionSpec {
        private final String connectionString;
        private final Properties properties;
    }

}
