package com.dremio.exec.store.jdbc;

import com.dremio.connector.metadata.extensions.*;
import javax.inject.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.server.*;
import com.dremio.exec.*;
import com.dremio.connector.metadata.*;
import com.dremio.service.namespace.capabilities.*;
import com.dremio.service.namespace.dataset.proto.*;
import com.dremio.service.namespace.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.exec.store.*;
import java.io.*;
import javax.sql.*;
import com.dremio.common.*;
import org.apache.calcite.schema.*;
import com.dremio.exec.planner.sql.*;
import com.dremio.exec.planner.types.*;
import com.dremio.exec.store.jdbc.legacy.*;
import org.apache.calcite.runtime.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.tablefunctions.*;
import com.dremio.exec.physical.base.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.common.expression.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.dremio.exec.record.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.calcite.rel.type.*;
import org.slf4j.*;
import java.sql.*;



import java.util.*;
import java.util.stream.*;

public class JdbcStoragePlugin implements StoragePlugin, SourceMetadata, SupportsListingDatasets, SupportsExternalQuery
{
    private static final Logger LOGGER;
    public static final BooleanCapability REQUIRE_TRIMS_ON_CHARS;
    public static final BooleanCapability COERCE_TIMES_TO_UTC;
    public static final BooleanCapability COERCE_TIMESTAMPS_TO_UTC;
    public static final BooleanCapability ADJUST_DATE_TIMEZONE;
    private final String name;
    private final Config config;
    private final Provider<StoragePluginId> pluginIdProvider;
    private final SabotContext context;
    private final int timeout;
    private JdbcDremioSqlDialect dialect;
    private JdbcSchemaFetcher fetcher;
    private CloseableDataSource source;
    
    public JdbcStoragePlugin(final Config config, final SabotContext context, final String name, final Provider<StoragePluginId> pluginIdProvider) {
        this.name = name;
        this.config = config;
        this.pluginIdProvider = pluginIdProvider;
        this.context = context;
        this.dialect = config.getDialect();
        this.timeout = (int)context.getOptionManager().getOption(ExecConstants.JDBC_ROW_COUNT_QUERY_TIMEOUT_VALIDATOR);
    }
    
    public boolean containerExists(final EntityPath containerPath) {
        return this.fetcher.containerExists(containerPath);
    }
    
    public DatasetHandleListing listDatasetHandles(final GetDatasetOption... options) {
        return this.fetcher.getTableHandles();
    }
    
    public Optional<DatasetHandle> getDatasetHandle(final EntityPath datasetPath, final GetDatasetOption... options) {
        if (datasetPath.size() < 2 || datasetPath.size() > 4) {
            return Optional.empty();
        }
        return this.fetcher.getTableHandle(datasetPath.getComponents());
    }
    
    public DatasetMetadata getDatasetMetadata(final DatasetHandle datasetHandle, final PartitionChunkListing chunkListing, final GetMetadataOption... options) {
        return this.fetcher.getTableMetadata(datasetHandle);
    }
    
    public PartitionChunkListing listPartitionChunks(final DatasetHandle datasetHandle, final ListPartitionChunkOption... options) {
        return this.fetcher.listPartitionChunks(datasetHandle);
    }
    
    public SourceCapabilities getSourceCapabilities() {
        if (this.dialect == null) {
            return new SourceCapabilities(new CapabilityValue[0]);
        }
        return new SourceCapabilities(new CapabilityValue[] { new BooleanCapabilityValue(SourceCapabilities.TREAT_CALCITE_SCAN_COST_AS_INFINITE, true), new BooleanCapabilityValue(SourceCapabilities.SUBQUERY_PUSHDOWNABLE, this.dialect.supportsSubquery()), new BooleanCapabilityValue(SourceCapabilities.CORRELATED_SUBQUERY_PUSHDOWN, this.dialect.supportsCorrelatedSubquery()), new BooleanCapabilityValue(JdbcStoragePlugin.REQUIRE_TRIMS_ON_CHARS, this.dialect.requiresTrimOnChars()), new BooleanCapabilityValue(JdbcStoragePlugin.COERCE_TIMES_TO_UTC, this.dialect.coerceTimesToUTC()), new BooleanCapabilityValue(JdbcStoragePlugin.COERCE_TIMESTAMPS_TO_UTC, this.dialect.coerceTimestampsToUTC()), new BooleanCapabilityValue(JdbcStoragePlugin.ADJUST_DATE_TIMEZONE, this.dialect.adjustDateTimezone()) });
    }
    
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return (Class<? extends StoragePluginRulesFactory>)this.context.getConfig().getClass("dremio.plugins.jdbc.rulesfactory", (Class)StoragePluginRulesFactory.class, (Class)JdbcRulesFactory.class);
    }
    
    public boolean hasAccessPermission(final String user, final NamespaceKey key, final DatasetConfig datasetConfig) {
        return true;
    }
    
    public SourceState getState() {
        if (null == this.source) {
            JdbcStoragePlugin.LOGGER.error("JDBC source {} has not been started.", this.name);
            return SourceState.badState(new String[] { String.format("JDBC source %s has not been started.", this.name) });
        }
        try (final Connection connection = this.source.getConnection()) {
            final boolean isValid = connection.isValid(1);
            if (isValid) {
                return SourceState.GOOD;
            }
            return SourceState.badState(new String[] { "Connection is not valid." });
        }
        catch (Exception e) {
            JdbcStoragePlugin.LOGGER.error("Connection is not valid.", (Throwable)e);
            return SourceState.badState(e);
        }
    }
    
    @Deprecated
    public ViewTable getView(final List<String> tableSchemaPath, final SchemaConfig schemaConfig) {
        return null;
    }
    
    public void start() throws IOException {
        if (this.dialect == null) {
            throw new RuntimeException("Failure instantiating the dialect for this source. Please see Dremio logs for more information.");
        }
        try {
            this.source = this.config.getDatasourceFactory().newDataSource();
        }
        catch (SQLException ex) {
            throw new IOException(StoragePluginUtils.generateSourceErrorMessage(this.name, ex.getMessage()), ex);
        }
        this.fetcher = this.config.getDialect().getSchemaFetcher(this.name, this.source, this.timeout, this.config);
    }
    
    public DataSource getSource() {
        return this.source;
    }
    
    public Config getConfig() {
        return this.config;
    }
    
    public String getName() {
        return this.name;
    }
    
    public JdbcDremioSqlDialect getDialect() {
        return this.dialect;
    }
    
    private StoragePluginId getPluginId() {
        return (StoragePluginId)this.pluginIdProvider.get();
    }
    
    public void close() throws Exception {
        AutoCloseables.close(new AutoCloseable[] { this.source });
    }
    
    public List<Function> getFunctions(final List<String> tableSchemaPath, final SchemaConfig schemaConfig) {
        final String sourceName = this.getPluginId().getName();
        if (this.config.allowExternalQuery) {
            return SupportsExternalQuery.getExternalQueryFunction(query -> JdbcExternalQueryMetadataUtility.getBatchSchema(this.source, this.dialect, query, sourceName), schema -> CalciteArrowHelper.wrap(schema).toCalciteRecordType(SqlTypeFactoryImpl.INSTANCE, f -> f.getType().getTypeID() != ArrowType.Null.TYPE_TYPE), this.getPluginId(), tableSchemaPath).map(Collections::singletonList).orElse(Collections.emptyList());
        }
        final String errorMsg = (this.dialect instanceof LegacyDialect) ? "External Query is not supported with legacy mode enabled on source" : "Permission denied to run External Query on source";
        throw newValidationError(errorMsg, sourceName);
    }
    
    private static CalciteContextException newValidationError(final String errorMsg, final String sourceName) {
        return SqlUtil.newContextException(SqlParserPos.ZERO, DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryNotSupportedError(errorMsg + " <" + sourceName + ">"));
    }
    
    public PhysicalOperator getExternalQueryPhysicalOperator(final PhysicalPlanCreator creator, final ExternalQueryScanPrel prel, final BatchSchema schema, final String sql) {
        final SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
        final ImmutableSet.Builder<String> skippedColumnsBuilder = new ImmutableSet.Builder();
        this.filterBatchSchema(schema, schemaBuilder, skippedColumnsBuilder);
        final BatchSchema filteredSchema = schemaBuilder.build();
        final ImmutableSet<String> skippedColumns = (ImmutableSet<String>)skippedColumnsBuilder.build();
        return new JdbcGroupScan(creator.props((Prel)prel, "$dremio$", schema, JdbcPrel.RESERVE, JdbcPrel.LIMIT), sql, filteredSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()), this.getPluginId(), filteredSchema, skippedColumns);
    }
    
    private void filterBatchSchema(final BatchSchema originalSchema, final SchemaBuilder filteredSchemaBuilder, final ImmutableSet.Builder<String> skippedColumnsBuilder) {
        for (final Field field : originalSchema) {
            if (field.getType().getTypeID() == ArrowType.Null.TYPE_TYPE) {
                skippedColumnsBuilder.add(field.getName());
            }
            else {
                filteredSchemaBuilder.addField(field);
            }
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)JdbcStoragePlugin.class);
        DriverManager.getDrivers();
        REQUIRE_TRIMS_ON_CHARS = new BooleanCapability("require_trims_on_chars", false);
        COERCE_TIMES_TO_UTC = new BooleanCapability("coerce_times_to_utc", false);
        COERCE_TIMESTAMPS_TO_UTC = new BooleanCapability("coerce_timestamps_to_utc", false);
        ADJUST_DATE_TIMEZONE = new BooleanCapability("adjust_date_timezone", false);
    }
    
    public static final class Config
    {
        private final JdbcDremioSqlDialect dialect;
        private final int fetchSize;
        private final CloseableDataSource.Factory datasourceFactory;
        private final String database;
        private final boolean showOnlyConnDatabase;
        private final Set<String> hiddenSchemas;
        private final Set<String> hiddenTableTypes;
        private final boolean skipSchemaDiscovery;
        private final boolean usePrepareForGetTables;
        private final boolean allowExternalQuery;
        
        private Config(final Builder builder) {
            this.dialect = Preconditions.checkNotNull(builder.dialect);
            this.datasourceFactory = (CloseableDataSource.Factory)Preconditions.checkNotNull(builder.datasourceFactory);
            this.fetchSize = builder.fetchSize;
            this.database = builder.database;
            this.showOnlyConnDatabase = builder.showOnlyConnDatabase;
            this.hiddenSchemas = unmodifiableCopyOf(builder.hiddenSchemas);
            this.hiddenTableTypes = unmodifiableCopyOf(builder.hiddenTableTypes);
            this.skipSchemaDiscovery = builder.skipSchemaDiscovery;
            this.usePrepareForGetTables = builder.usePrepareForGetTables;
            this.allowExternalQuery = builder.allowExternalQuery;
        }
        
        private static Set<String> unmodifiableCopyOf(final Set<String> set) {
            final Set<String> result = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            result.addAll(set);
            return Collections.unmodifiableSet((Set<? extends String>)result);
        }
        
        public static Builder newBuilder() {
            return new Builder();
        }
        
        public JdbcDremioSqlDialect getDialect() {
            return this.dialect;
        }
        
        public int getFetchSize() {
            return this.fetchSize;
        }
        
        public CloseableDataSource.Factory getDatasourceFactory() {
            return this.datasourceFactory;
        }
        
        public String getDatabase() {
            return this.database;
        }
        
        public boolean showOnlyConnDatabase() {
            return this.showOnlyConnDatabase;
        }
        
        public Set<String> getHiddenSchemas() {
            return this.hiddenSchemas;
        }
        
        public Set<String> getHiddenTableTypes() {
            return this.hiddenTableTypes;
        }
        
        public boolean shouldSkipSchemaDiscovery() {
            return this.skipSchemaDiscovery;
        }
        
        public boolean usePrepareForGetTables() {
            return this.usePrepareForGetTables;
        }
        
        public boolean allowExternalQuery() {
            return this.allowExternalQuery;
        }
        
        public static final class Builder
        {
            public static final Set<String> DEFAULT_HIDDEN_SCHEMAS;
            public static final Set<String> DEFAULT_HIDDEN_TABLE_TYPES;
            private JdbcDremioSqlDialect dialect;
            private int fetchSize;
            private String database;
            private boolean showOnlyConnDatabase;
            private CloseableDataSource.Factory datasourceFactory;
            private final Set<String> hiddenSchemas;
            private final Set<String> hiddenTableTypes;
            private boolean skipSchemaDiscovery;
            private boolean usePrepareForGetTables;
            private boolean allowExternalQuery;
            
            private static Set<String> newUnmodifiableSet(final String... strings) {
                return Collections.unmodifiableSet(Arrays.stream(strings).collect(Collectors.toCollection(() -> new TreeSet<String>(String.CASE_INSENSITIVE_ORDER))));
            }
            
            private Builder() {
                this.hiddenSchemas = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                this.hiddenTableTypes = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                this.allowExternalQuery = false;
                this.hiddenSchemas.addAll(Builder.DEFAULT_HIDDEN_SCHEMAS);
                this.hiddenTableTypes.addAll(Builder.DEFAULT_HIDDEN_TABLE_TYPES);
            }
            
            public Builder withDialect(final JdbcDremioSqlDialect dialect) {
                this.dialect = (JdbcDremioSqlDialect)Preconditions.checkNotNull(dialect);
                return this;
            }
            
            public Builder withFetchSize(final int fetchSize) {
                this.fetchSize = fetchSize;
                return this;
            }
            
            public Builder withDatabase(final String database) {
                this.database = database;
                return this;
            }
            
            public Builder withShowOnlyConnDatabase(final boolean showOnlyConnDatabase) {
                this.showOnlyConnDatabase = showOnlyConnDatabase;
                return this;
            }
            
            public Builder withDatasourceFactory(final CloseableDataSource.Factory factory) {
                this.datasourceFactory = factory;
                return this;
            }
            
            public Builder addHiddenSchema(final String schema, final String... others) {
                this.hiddenSchemas.add(schema);
                if (others != null) {
                    this.hiddenSchemas.addAll(Arrays.asList(others));
                }
                return this;
            }
            
            public Builder clearHiddenSchemas() {
                this.hiddenSchemas.clear();
                return this;
            }
            
            public Builder addHiddenTableType(final String tableType, final String... others) {
                this.hiddenTableTypes.add(tableType);
                if (others != null) {
                    this.hiddenSchemas.addAll(Arrays.asList(others));
                }
                return this;
            }
            
            public Builder clearHiddenTableTypes() {
                this.hiddenTableTypes.clear();
                return this;
            }
            
            public Builder withSkipSchemaDiscovery(final boolean skipSchemaDiscovery) {
                this.skipSchemaDiscovery = skipSchemaDiscovery;
                return this;
            }
            
            public Builder withPrepareForGetTables(final boolean prepareForGetTables) {
                this.usePrepareForGetTables = prepareForGetTables;
                return this;
            }
            
            public Builder withAllowExternalQuery(final boolean allowExternalQuery) {
                this.allowExternalQuery = allowExternalQuery;
                return this;
            }
            
            public Config build() {
                return new Config(this);
            }
            
            static {
                DEFAULT_HIDDEN_SCHEMAS = newUnmodifiableSet("SYS", "INFORMATION_SCHEMA");
                DEFAULT_HIDDEN_TABLE_TYPES = newUnmodifiableSet("INDEX", "SEQUENCE", "SYSTEM INDEX", "SYSTEM VIEW", "SYSTEM TOAST INDEX", "SYSTEM TOAST TABLE", "SYSTEM TABLE", "TEMPORARY TABLE", "TEMPORARY VIEW", "TYPE");
            }
        }
    }
}
