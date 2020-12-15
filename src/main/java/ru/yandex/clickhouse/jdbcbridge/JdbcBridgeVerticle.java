/**
 * Copyright 2019-2020, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.yandex.clickhouse.jdbcbridge;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.PrometheusScrapingHandler;
import io.vertx.micrometer.VertxPrometheusOptions;
import ru.yandex.clickhouse.jdbcbridge.core.ByteBuffer;
import ru.yandex.clickhouse.jdbcbridge.core.ColumnDefinition;
import ru.yandex.clickhouse.jdbcbridge.core.TableDefinition;
import ru.yandex.clickhouse.jdbcbridge.core.DataType;
import ru.yandex.clickhouse.jdbcbridge.core.Extension;
import ru.yandex.clickhouse.jdbcbridge.core.ExtensionManager;
import ru.yandex.clickhouse.jdbcbridge.core.NamedDataSource;
import ru.yandex.clickhouse.jdbcbridge.core.NamedQuery;
import ru.yandex.clickhouse.jdbcbridge.core.NamedSchema;
import ru.yandex.clickhouse.jdbcbridge.core.QueryParameters;
import ru.yandex.clickhouse.jdbcbridge.core.QueryParser;
import ru.yandex.clickhouse.jdbcbridge.core.Repository;
import ru.yandex.clickhouse.jdbcbridge.core.RepositoryManager;
import ru.yandex.clickhouse.jdbcbridge.core.ResponseWriter;
import ru.yandex.clickhouse.jdbcbridge.core.Utils;
import ru.yandex.clickhouse.jdbcbridge.impl.ConfigDataSource;
import ru.yandex.clickhouse.jdbcbridge.impl.JdbcDataSource;
import ru.yandex.clickhouse.jdbcbridge.impl.JsonFileRepository;
import ru.yandex.clickhouse.jdbcbridge.impl.ScriptDataSource;

import static ru.yandex.clickhouse.jdbcbridge.core.DataType.*;

/**
 * Unified data source bridge for ClickHouse.
 *
 * @since 2.0
 */
public class JdbcBridgeVerticle extends AbstractVerticle implements ExtensionManager {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcBridgeVerticle.class);

    private static long startTime;

    private static final String CONFIG_PATH = Utils.getConfiguration("config", "CONFIG_DIR", "jdbc-bridge.config.dir");

    private static final int DEFAULT_SERVER_PORT = 9019;

    private static final String RESPONSE_CONTENT_TYPE = "application/octet-stream";

    private static final String WRITE_RESPONSE = "Ok.";
    private static final String PING_RESPONSE = WRITE_RESPONSE + "\n";

    private final List<Extension<?>> extensions;

    private final RepositoryManager repos;

    private long scanInterval = 5000L;

    List<Repository<?>> loadRepositories(JsonObject serverConfig) {
        List<Repository<?>> repos = new ArrayList<>();

        Extension<Repository> defaultExt = new Extension<>(JsonFileRepository.class);
        JsonArray declaredRepos = serverConfig == null ? null : serverConfig.getJsonArray("repositories");
        if (declaredRepos == null) {
            // let's go with default extensions
            declaredRepos = new JsonArray();

            declaredRepos.add(NamedDataSource.class.getName());
            declaredRepos.add(NamedSchema.class.getName());
            declaredRepos.add(NamedQuery.class.getName());
        }

        for (Object item : declaredRepos) {
            Repository<?> repo = null;

            if (item instanceof String) {
                repo = defaultExt.newInstance(this, defaultExt.loadClass(String.valueOf(item)));
            } else {
                JsonObject o = (JsonObject) item;

                String entityClassName = o.getString("entity");
                if (entityClassName == null || entityClassName.isEmpty()) {
                    continue;
                }

                String repoClassName = o.getString("repository");

                ArrayList<String> urls = null;
                JsonArray libUrls = o.getJsonArray("libUrls");
                if (repoClassName != null && !repoClassName.isEmpty() && libUrls != null) {
                    urls = new ArrayList<>(libUrls.size());
                    for (Object u : libUrls) {
                        if (u instanceof String) {
                            urls.add((String) u);
                        }
                    }
                }

                Extension<?> ext = Utils.loadExtension(urls, repoClassName);
                if (ext != null) {
                    repo = (Repository<?>) ext.newInstance(this, ext.loadClass(entityClassName));
                }
            }

            if (repo != null) {
                repos.add(repo);
            }
        }

        return repos;
    }

    List<Extension<?>> loadExtensions(JsonObject serverConfig) {
        List<Extension<?>> exts = new ArrayList<>();

        JsonArray declaredExts = serverConfig == null ? null : serverConfig.getJsonArray("extensions");
        if (declaredExts == null) {
            // let's go with default extensions
            declaredExts = new JsonArray();

            declaredExts.add(JdbcDataSource.class.getName());
            declaredExts.add(ConfigDataSource.class.getName());
            declaredExts.add(ScriptDataSource.class.getName());
        }

        for (Object item : declaredExts) {
            Extension<?> ext = null;

            if (item instanceof String) {
                ext = Utils.loadExtension((String) item);
            } else {
                JsonObject o = (JsonObject) item;

                String className = o.getString("class");

                JsonArray libUrls = o.getJsonArray("libUrls");
                if (libUrls != null) {
                    ArrayList<String> urls = new ArrayList<>(libUrls.size());
                    for (Object u : libUrls) {
                        if (u instanceof String) {
                            urls.add((String) u);
                        }
                    }

                    // FIXME duplicated extensions?
                    ext = Utils.loadExtension(urls, className);
                } else {
                    ext = Utils.loadExtension(className);
                }
            }

            if (ext != null) {
                exts.add(ext);
            }
        }

        return exts;
    }

    public JdbcBridgeVerticle() {
        super();

        this.extensions = new ArrayList<>();

        this.repos = Utils.loadService(RepositoryManager.class);
    }

    @Override
    public void start() {
        JsonObject config = Utils.loadJsonFromFile(Paths
                .get(CONFIG_PATH,
                        Utils.getConfiguration("server.json", "SERVER_CONFIG_FILE", "jdbc-bridge.server.config.file"))
                .toString());

        this.scanInterval = config.getLong("configScanPeriod", 5000L);

        this.repos.update(this.loadRepositories(config));
        // extension must be loaded *after* repository is initialized
        this.extensions.addAll(this.loadExtensions(config));

        // initialize extensions so that they're fully ready for use
        for (Extension<?> ext : this.extensions) {
            ext.initialize(this);
        }

        startServer(config,
                Utils.loadJsonFromFile(Paths.get(CONFIG_PATH,
                        Utils.getConfiguration("httpd.json", "HTTPD_CONFIG_FILE", "jdbc-bridge.httpd.config.file"))
                        .toString()));
    }

    private void startServer(JsonObject bridgeServerConfig, JsonObject httpServerConfig) {
        if (httpServerConfig.isEmpty()) {
            // make sure we can pass long query/script by default
            httpServerConfig.put("maxInitialLineLength", 2147483647L);
        }

        HttpServer server = vertx.createHttpServer(new HttpServerOptions(httpServerConfig));
        // vertx.createHttpServer(new
        // HttpServerOptions().setTcpNoDelay(false).setTcpKeepAlive(true)
        // .setTcpFastOpen(true).setLogActivity(true));

        long requestTimeout = bridgeServerConfig.getLong("requestTimeout", 5000L);
        long queryTimeout = Math.max(requestTimeout, bridgeServerConfig.getLong("queryTimeout", 30000L));

        TimeoutHandler requestTimeoutHandler = TimeoutHandler.create(requestTimeout);
        TimeoutHandler queryTimeoutHandler = TimeoutHandler.create(queryTimeout);

        // https://github.com/vert-x3/vertx-examples/blob/master/web-examples/src/main/java/io/vertx/example/web/mongo/Server.java
        Router router = Router.router(vertx);

        router.route("/metrics").handler(PrometheusScrapingHandler.create());

        router.route().handler(BodyHandler.create()).handler(this::responseHandlers)
                .handler(ResponseContentTypeHandler.create()).failureHandler(this::errorHandler);

        // stateless endpoints
        router.get("/ping").handler(requestTimeoutHandler).handler(this::handlePing);

        router.post("/identifier_quote").produces(RESPONSE_CONTENT_TYPE).handler(requestTimeoutHandler)
                .handler(this::handleIdentifierQuote);
        router.post("/columns_info").produces(RESPONSE_CONTENT_TYPE).handler(queryTimeoutHandler)
                .handler(this::handleColumnsInfo);
        router.post("/").produces(RESPONSE_CONTENT_TYPE).handler(queryTimeoutHandler).handler(this::handleQuery);
        router.post("/write").produces(RESPONSE_CONTENT_TYPE).handler(queryTimeoutHandler).handler(this::handleWrite);

        log.info("Starting web server...");
        int port = bridgeServerConfig.getInteger("serverPort", DEFAULT_SERVER_PORT);
        server.requestHandler(router).listen(port, action -> {
            if (action.succeeded()) {
                log.info("Server http://0.0.0.0:{} started in {} ms", port, System.currentTimeMillis() - startTime);
            } else {
                log.error("Failed to start server", action.cause());
            }
        });
    }

    private void responseHandlers(RoutingContext ctx) {
        HttpServerRequest req = ctx.request();

        String path = ctx.normalisedPath();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Context:\n{}", path, ctx.data());
            log.debug("[{}] Headers:\n{}", path, req.headers());
            log.debug("[{}] Parameters:\n{}", path, req.params());
        }

        if (log.isTraceEnabled()) {
            log.trace("[{}] Body:\n{}", path, ctx.getBodyAsString());
        }

        HttpServerResponse resp = ctx.response();

        resp.endHandler(handler -> {
            if (log.isTraceEnabled()) {
                log.trace("[{}] About to end response...", ctx.normalisedPath());
            }
        });

        resp.closeHandler(handler -> {
            if (log.isTraceEnabled()) {
                log.trace("[{}] About to close response...", ctx.normalisedPath());
            }
        });

        resp.drainHandler(handler -> {
            if (log.isTraceEnabled()) {
                log.trace("[{}] About to drain response...", ctx.normalisedPath());
            }
        });

        resp.exceptionHandler(throwable -> {
            log.error("Caught exception", throwable);
        });

        ctx.next();
    }

    private void errorHandler(RoutingContext ctx) {
        log.error("Failed to respond", ctx.failure());
        ctx.response().setStatusCode(500).end(ctx.failure().getMessage());
    }

    private void handlePing(RoutingContext ctx) {
        ctx.response().end(PING_RESPONSE);
    }

    private NamedDataSource getDataSource(String uri, boolean orCreate) {
        return getDataSource(getDataSourceRepository(), uri, orCreate);
    }

    private NamedDataSource getDataSource(Repository<NamedDataSource> repo, String uri, boolean orCreate) {
        NamedDataSource ds = repo.get(uri);

        return ds == null && orCreate ? new NamedDataSource(uri, null, null) : ds;
    }

    private void handleColumnsInfo(RoutingContext ctx) {
        final QueryParser parser = QueryParser.fromRequest(ctx, getDataSourceRepository());

        String rawQuery = parser.getRawQuery();

        log.info("Raw query:\n{}", rawQuery);

        String uri = parser.getConnectionString();

        QueryParameters params = parser.getQueryParameters();
        NamedDataSource ds = getDataSource(uri, params.isDebug());
        String dsId = uri;
        if (ds != null) {
            dsId = ds.getId();
            params = ds.newQueryParameters(params);
        }

        // even it's a named query, the column list could be empty
        NamedQuery namedQuery = getQueryRepository().get(rawQuery);
        // priority: name query -> named schema -> type inferring
        NamedSchema namedSchema = getSchemaRepository().get(parser.getNormalizedSchema());

        TableDefinition tableDef;
        if (namedQuery != null) {
            if (namedSchema == null) {
                namedSchema = getSchemaRepository().get(namedQuery.getSchema());
            }

            tableDef = namedQuery.getColumns(params);
        } else {
            tableDef = namedSchema != null ? namedSchema.getColumns()
                    : ds.getResultColumns(parser.getSchema(), parser.getNormalizedQuery(), params);
        }

        List<ColumnDefinition> additionalColumns = new ArrayList<ColumnDefinition>();
        if (params.showDatasourceColumn()) {
            additionalColumns.add(new ColumnDefinition(TableDefinition.COLUMN_DATASOURCE, DataType.Str, true,
                    DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE, null, dsId, null));
        }
        if (params.showCustomColumns() && ds != null) {
            additionalColumns.addAll(ds.getCustomColumns());
        }

        if (additionalColumns.size() > 0) {
            tableDef = new TableDefinition(tableDef, true,
                    additionalColumns.toArray(new ColumnDefinition[additionalColumns.size()]));
        }

        String columnsInfo = tableDef.toString();

        if (log.isDebugEnabled()) {
            log.debug("Columns info:\n[{}]", columnsInfo);
        }
        ctx.response().end(ByteBuffer.asBuffer(columnsInfo));
    }

    private void handleIdentifierQuote(RoutingContext ctx) {
        ctx.response().end(ByteBuffer.asBuffer(NamedDataSource.DEFAULT_QUOTE_IDENTIFIER));
    }

    private void handleQuery(RoutingContext ctx) {
        final Repository<NamedDataSource> manager = getDataSourceRepository();
        final QueryParser parser = QueryParser.fromRequest(ctx, manager);

        ctx.response().setChunked(true);

        vertx.executeBlocking(promise -> {
            if (log.isTraceEnabled()) {
                log.trace("About to execute query...");
            }

            QueryParameters params = parser.getQueryParameters();
            NamedDataSource ds = getDataSource(manager, parser.getConnectionString(), params.isDebug());
            params = ds.newQueryParameters(params);

            String schema = parser.getSchema();
            NamedSchema namedSchema = getSchemaRepository().get(parser.getNormalizedSchema());

            String generatedQuery = parser.getRawQuery();
            String normalizedQuery = parser.getNormalizedQuery();
            // try if it's a named query first
            NamedQuery namedQuery = getQueryRepository().get(normalizedQuery);
            // in case the "query" is a local file...
            normalizedQuery = ds.loadSavedQueryAsNeeded(normalizedQuery, params);

            if (log.isDebugEnabled()) {
                log.debug("Generated query:\n{}\nNormalized query:\n{}", generatedQuery, normalizedQuery);
            }

            final HttpServerResponse resp = ctx.response();

            ResponseWriter writer = new ResponseWriter(resp, parser.getStreamOptions(),
                    ds.getQueryTimeout(params.getTimeout()));

            long executionStartTime = System.currentTimeMillis();
            if (namedQuery != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Found named query: [{}]", namedQuery);
                }

                if (namedSchema == null) {
                    namedSchema = getSchemaRepository().get(namedQuery.getSchema());
                }
                // columns in request might just be a subset of defined list
                // for example:
                // - named query 'test' is: select a, b, c from table
                // - clickhouse query: select b, a from jdbc('?','','test')
                // - requested columns: b, a
                ds.executeQuery(schema, namedQuery, namedSchema != null ? namedSchema.getColumns() : parser.getTable(),
                        params, writer);
            } else {
                // columnsInfo could be different from what we responded earlier, so let's parse
                // it again
                Boolean containsWhitespace = ds.undertandsSQL() ? null : Boolean.TRUE;
                if (containsWhitespace == null) { // limit this weird behaviour to SQL datasources
                    for (int i = 0; i < normalizedQuery.length(); i++) {
                        char ch = normalizedQuery.charAt(i);
                        if (Character.isWhitespace(ch)) {
                            if (containsWhitespace != null) {
                                containsWhitespace = Boolean.TRUE;
                                break;
                            }
                        } else if (containsWhitespace == null) {
                            containsWhitespace = Boolean.FALSE;
                        }
                    }
                }

                TableDefinition queryColumns = namedSchema != null ? namedSchema.getColumns() : parser.getTable();
                // unfortunately default values will be lost between two requests, so we have to
                // add it back...
                List<ColumnDefinition> additionalColumns = new ArrayList<ColumnDefinition>();
                if (params.showDatasourceColumn()) {
                    additionalColumns.add(new ColumnDefinition(TableDefinition.COLUMN_DATASOURCE, DataType.Str, true,
                            DEFAULT_LENGTH, DEFAULT_PRECISION, DEFAULT_SCALE, null, ds.getId(), null));
                }
                if (params.showCustomColumns()) {
                    additionalColumns.addAll(ds.getCustomColumns());
                }

                queryColumns.updateValues(additionalColumns);

                ds.executeQuery(schema, parser.getNormalizedQuery(),
                        Boolean.TRUE.equals(containsWhitespace) ? normalizedQuery : generatedQuery, queryColumns,
                        params, writer);
            }

            if (log.isDebugEnabled()) {
                log.debug("Completed execution in {} ms.", System.currentTimeMillis() - executionStartTime);
            }

            promise.complete();
        }, false, res -> {
            if (res.succeeded()) {
                if (log.isDebugEnabled()) {
                    log.debug("Wrote back query result");
                }
                ctx.response().end();
            } else {
                ctx.fail(res.cause());
            }
        });
    }

    // https://github.com/ClickHouse/ClickHouse/blob/bee5849c6a7dba20dbd24dfc5fd5a786745d90ff/programs/odbc-bridge/MainHandler.cpp#L169
    private void handleWrite(RoutingContext ctx) {
        final Repository<NamedDataSource> manager = getDataSourceRepository();
        final QueryParser parser = QueryParser.fromRequest(ctx, manager, true);

        ctx.response().setChunked(true);

        vertx.executeBlocking(promise -> {
            if (log.isTraceEnabled()) {
                log.trace("About to execute mutation...");
            }

            QueryParameters params = parser.getQueryParameters();
            NamedDataSource ds = getDataSource(manager, parser.getConnectionString(), params.isDebug());
            params = ds == null ? params : ds.newQueryParameters(params);

            // final HttpServerRequest req = ctx.request();
            final HttpServerResponse resp = ctx.response();

            final String generatedQuery = parser.getRawQuery();

            String normalizedQuery = parser.getNormalizedQuery();
            if (log.isDebugEnabled()) {
                log.debug("Generated query:\n{}\nNormalized query:\n{}", generatedQuery, normalizedQuery);
            }

            // try if it's a named query first
            NamedQuery namedQuery = getQueryRepository().get(normalizedQuery);
            // in case the "query" is a local file...
            normalizedQuery = ds.loadSavedQueryAsNeeded(normalizedQuery, params);

            // TODO: use named schema as table name?

            String table = parser.getRawQuery();
            if (namedQuery != null) {
                table = parser.extractTable(ds.loadSavedQueryAsNeeded(namedQuery.getQuery(), params));
            } else {
                table = parser.extractTable(ds.loadSavedQueryAsNeeded(normalizedQuery, params));
            }

            ds.executeMutation(parser.getSchema(), table, parser.getTable(), params, ByteBuffer.wrap(ctx.getBody()));

            resp.write(ByteBuffer.asBuffer(WRITE_RESPONSE));

            promise.complete();
        }, false, res -> {
            if (res.succeeded()) {
                if (log.isDebugEnabled()) {
                    log.debug("Wrote back query result");
                }
                ctx.response().end();
            } else {
                ctx.fail(res.cause());
            }
        });
    }

    private Repository<NamedDataSource> getDataSourceRepository() {
        return getRepositoryManager().getRepository(NamedDataSource.class);
    }

    private Repository<NamedSchema> getSchemaRepository() {
        return getRepositoryManager().getRepository(NamedSchema.class);
    }

    private Repository<NamedQuery> getQueryRepository() {
        return getRepositoryManager().getRepository(NamedQuery.class);
    }

    @Override
    public <T> Extension<T> getExtension(Class<? extends T> clazz) {
        String className = Objects.requireNonNull(clazz).getName();

        Extension<T> ext = null;

        for (Extension<?> e : this.extensions) {
            if (e.getProviderClass().getName().equals(className)) {
                ext = (Extension<T>) e;
            }
        }

        return ext;
    }

    @Override
    public RepositoryManager getRepositoryManager() {
        return this.repos;
    }

    @Override
    public void registerConfigLoader(String configPath, Consumer<JsonObject> consumer) {
        final String path = Paths.get(CONFIG_PATH, configPath).toString();

        log.info("Start to monitor configuration file(s) at [{}]", path);

        ConfigRetriever retriever = ConfigRetriever.create(vertx,
                new ConfigRetrieverOptions().setScanPeriod(this.scanInterval)
                        .addStore(new ConfigStoreOptions().setType("directory")
                                .setConfig(new JsonObject().put("path", path).put("filesets", new JsonArray()
                                        .add(new JsonObject().put("pattern", "*.json").put("format", "json"))))));

        retriever.getConfig(action -> {
            if (action.succeeded()) {
                vertx.executeBlocking(promise -> {
                    consumer.accept(action.result());
                }, true, res -> {
                    if (!res.succeeded()) {
                        log.error("Failed to load configuration", res.cause());
                    }
                });
            } else {
                log.warn("Not able to load configuration from [{}] due to {}", path, action.cause().getMessage());
            }
        });

        retriever.listen(change -> {
            log.info("Configuration change in [{}] detected", path);

            vertx.executeBlocking(promise -> {
                consumer.accept(change.getNewConfiguration());
            }, true, res -> {
                if (!res.succeeded()) {
                    log.error("Failed to reload configuration", res.cause());
                }
            });
        });
    }

    @Override
    public Map<String, Object> getScriptableObjects() {
        Map<String, Object> vars = new HashMap<>();

        // TODO and some utilities?
        vars.put("__vertx", vertx);
        vars.put("__datasources", getDataSourceRepository());
        vars.put("__schemas", getSchemaRepository());
        vars.put("__queries", getQueryRepository());

        return vars;
    }

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();

        MeterRegistry registry = Utils.getDefaultMetricRegistry();
        new ClassLoaderMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);

        // https://github.com/eclipse-vertx/vert.x/blob/master/src/main/generated/io/vertx/core/VertxOptionsConverter.java
        Vertx vertx = Vertx.vertx(new VertxOptions(Utils.loadJsonFromFile(Paths
                .get(CONFIG_PATH,
                        Utils.getConfiguration("vertx.json", "VERTX_CONFIG_FILE", "jdbc-bridge.vertx.config.file"))
                .toString()))
                        .setMetricsOptions(new MicrometerMetricsOptions()
                                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                                .setMicrometerRegistry(registry).setEnabled(true)));

        vertx.deployVerticle(new JdbcBridgeVerticle());
    }
}
