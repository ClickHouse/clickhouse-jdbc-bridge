package ru.yandex.clickhouse.jdbcbridge.db.clickhouse.db;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.jooq.AlterTableFinalStep;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by krash on 19.11.18.
 */
@Slf4j
public class OracleIntegrationTest extends ExternalDBTest {

    private static final Map<String, String> SUBSTITUTIONS = new HashMap<>();

    static {
        SUBSTITUTIONS.put(" tinyint ", " NUMBER(3) ");
        SUBSTITUTIONS.put(" bigint ", " NUMBER(19) ");
        SUBSTITUTIONS.put(" double ", " DOUBLE PRECISION ");
    }

    @Override
    protected boolean isTypeSupported(DataType<?> type) {
        return !Sets.newHashSet("time", "boolean", "bit").contains(type.getCastTypeName());
    }

    @Override
    protected String getEnvPropertyName() {
        return "datasource.oracle";
    }

    @Override
    protected ConnectionSpec getConnectionSpec() {
        Properties props = new Properties();
        props.put("user", "test");
        props.put("password", "test");
        return new ConnectionSpec("jdbc:oracle:thin:@localhost:1521:xe", props);
    }

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.HSQLDB;
    }

    protected void addField(DSLContext ctx, Connection conn, Field<?> field) throws Exception {
        AlterTableFinalStep alter = ctx.alterTable(getTableName()).add(field);
        String sql = alter.getSQL(ParamType.INLINED);

        for (Map.Entry<String, String> entry : SUBSTITUTIONS.entrySet()) {
            sql = sql.replaceFirst(entry.getKey(), entry.getValue());
        }

        log.info(sql);
        conn.createStatement().execute(sql);
    }
}
