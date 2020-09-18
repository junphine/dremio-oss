package com.dremio.exec.store.jdbc;

import javax.sql.*;
import com.dremio.connector.metadata.*;
import com.google.common.base.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.common.dialect.*;
import com.dremio.exec.store.*;
import java.sql.*;
import org.slf4j.*;
import java.util.*;
import com.google.common.collect.*;
import com.dremio.common.*;

public class JdbcSchemaFetcher
{
    private static final Logger logger;
    protected static final long BIG_ROW_COUNT = 1000000000L;
    protected static final Joiner PERIOD_JOINER;
    protected final DataSource dataSource;
    protected final String storagePluginName;
    protected final int timeout;
    protected final JdbcStoragePlugin.Config config;
    
    public JdbcSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        this.dataSource = dataSource;
        this.storagePluginName = name;
        this.timeout = timeout;
        this.config = config;
    }
    
    public boolean containerExists(final EntityPath containerPath) {
        try {
            final List<String> path = (List<String>)containerPath.getComponents();
            if (path.size() == 2) {
                return this.getCatalogsOrSchemas().contains(path.get(1));
            }
            if (path.size() == 3) {
                return this.getSchemas(path.get(1)).contains(path.get(2));
            }
            return path.size() == 1;
        }
        catch (Exception e) {
            JdbcSchemaFetcher.logger.error("Exception caught while checking if container exists.", (Throwable)e);
            return false;
        }
    }
    
    protected long getRowCount(final JdbcDatasetHandle handle) {
        final String quotedPath = this.getQuotedPath(handle.getDatasetPath());
        JdbcSchemaFetcher.logger.debug("Getting row count for table {}. ", (Object)quotedPath);
        final Optional<Long> count = this.executeQueryAndGetFirstLong("select count(*) from " + this.getQuotedPath(handle.getIdentifiers()));
        if (count.isPresent()) {
            return count.get();
        }
        JdbcSchemaFetcher.logger.debug("There was a problem getting the row count for table {}, using default of {}.", (Object)quotedPath, (Object)1000000000L);
        return 1000000000L;
    }
    
    public DatasetHandleListing getTableHandles() {
        if (this.config.shouldSkipSchemaDiscovery()) {
            JdbcSchemaFetcher.logger.debug("Skip schema discovery enabled, skipping getting tables '{}'", (Object)this.storagePluginName);
            return (DatasetHandleListing)new EmptyDatasetHandleListing();
        }
        JdbcSchemaFetcher.logger.debug("Getting all tables for plugin '{}'", (Object)this.storagePluginName);
        return (DatasetHandleListing)new JdbcIteratorListing((Iterator)new JdbcDatasetMetadataIterable(this.storagePluginName, this.dataSource, this.config));
    }
    
    public Optional<DatasetHandle> getTableHandle(final List<String> tableSchemaPath) {
        try {
            final Connection connection = this.dataSource.getConnection();
            Throwable x0 = null;
            try {
                if ((this.usePrepareForColumnMetadata() && this.config.shouldSkipSchemaDiscovery()) || this.usePrepareForGetTables()) {
                    return this.getTableHandleViaPrepare(tableSchemaPath, connection);
                }
                return this.getDatasetHandleViaGetTables(tableSchemaPath, connection);
            }
            catch (Throwable t) {
                x0 = t;
                throw t;
            }
            finally {
                if (connection != null) {
                    $closeResource(x0, connection);
                }
            }
        }
        catch (SQLException e) {
            JdbcSchemaFetcher.logger.warn("Failed to fetch schema for {}.", (Object)tableSchemaPath);
            return Optional.empty();
        }
    }
    
    public DatasetMetadata getTableMetadata(final DatasetHandle datasetHandle) {
        return (DatasetMetadata)new JdbcDatasetMetadata(this, (JdbcDatasetHandle)datasetHandle.unwrap((Class)JdbcDatasetHandle.class));
    }
    
    public PartitionChunkListing listPartitionChunks(final DatasetHandle datasetHandle) {
        return (PartitionChunkListing)new JdbcDatasetMetadata(this, (JdbcDatasetHandle)datasetHandle.unwrap((Class)JdbcDatasetHandle.class));
    }
    
    protected boolean usePrepareForColumnMetadata() {
        return false;
    }
    
    protected boolean usePrepareForGetTables() {
        return false;
    }
    
    protected final Optional<Long> executeQueryAndGetFirstLong(final String sql) {
        try {
            final Connection connection = this.dataSource.getConnection();
            Throwable x0 = null;
            try {
                final Statement statement = connection.createStatement();
                Throwable x2 = null;
                try {
                    statement.setFetchSize(this.config.getFetchSize());
                    statement.setQueryTimeout(this.timeout);
                    final ResultSet resultSet = statement.executeQuery(sql);
                    Throwable x3 = null;
                    try {
                        final ResultSetMetaData meta = resultSet.getMetaData();
                        final int colCount = meta.getColumnCount();
                        if (colCount != 1 || !resultSet.next()) {
                            JdbcSchemaFetcher.logger.debug("Invalid results returned for `{}`, colCount = {}.", (Object)sql, (Object)colCount);
                            return Optional.empty();
                        }
                        final long numRows = resultSet.getLong(1);
                        if (resultSet.wasNull()) {
                            return Optional.empty();
                        }
                        JdbcSchemaFetcher.logger.debug("Query `{}` returned {} rows.", (Object)sql, (Object)numRows);
                        return Optional.of(numRows);
                    }
                    catch (Throwable t) {
                        x3 = t;
                        throw t;
                    }
                    finally {
                        if (resultSet != null) {
                            $closeResource(x3, resultSet);
                        }
                    }
                }
                catch (Throwable t2) {
                    x2 = t2;
                    throw t2;
                }
                finally {
                    if (statement != null) {
                        $closeResource(x2, statement);
                    }
                }
            }
            catch (Throwable t3) {
                x0 = t3;
                throw t3;
            }
            finally {
                if (connection != null) {
                    $closeResource(x0, connection);
                }
            }
        }
        catch (Exception e) {
            JdbcSchemaFetcher.logger.warn("Took longer than {} seconds to execute query `{}`.", new Object[] { this.timeout, sql, e });
            return Optional.empty();
        }
    }
    
    protected static String getJoinedSchema(final String catalogName, final String schemaName, final String tableName) {
        final List<String> schemaPathThatFailed = new ArrayList<String>();
        if (!Strings.isNullOrEmpty(catalogName)) {
            schemaPathThatFailed.add(catalogName);
        }
        if (!Strings.isNullOrEmpty(schemaName)) {
            schemaPathThatFailed.add(schemaName);
        }
        if (!Strings.isNullOrEmpty(tableName) && !"%".equals(tableName)) {
            schemaPathThatFailed.add(tableName);
        }
        return JdbcSchemaFetcher.PERIOD_JOINER.join((Iterable)schemaPathThatFailed);
    }
    
    protected static List<String> getSchemas(final DatabaseMetaData metaData, final String catalogName, final JdbcStoragePlugin.Config config, final List<String> failed) {
        final ImmutableList.Builder<String> builder = (ImmutableList.Builder<String>)ImmutableList.builder();
        JdbcSchemaFetcher.logger.debug("Getting schemas for catalog=[{}].", (Object)catalogName);
        try {
            final ResultSet getSchemasResultSet = Strings.isNullOrEmpty(catalogName) ? metaData.getSchemas() : metaData.getSchemas(catalogName, null);
            Throwable x0 = null;
            try {
                while (getSchemasResultSet.next()) {
                    final String schema = getSchemasResultSet.getString(1);
                    if (config.getHiddenSchemas().contains(schema)) {
                        continue;
                    }
                    builder.add((Object)schema);
                }
            }
            catch (Throwable t) {
                x0 = t;
                throw t;
            }
            finally {
                if (getSchemasResultSet != null) {
                    $closeResource(x0, getSchemasResultSet);
                }
            }
        }
        catch (SQLException e) {
            failed.add(getJoinedSchema(catalogName, null, null));
        }
        return (List<String>)builder.build();
    }
    
    protected final String getQuotedPath(final EntityPath path) {
        return this.getQuotedPath(path.getComponents());
    }
    
    protected final String getQuotedPath(final List<String> tablePath) {
        final String[] pathSegments = tablePath.stream().map(path -> this.config.getDialect().quoteIdentifier(path)).toArray(String[]::new);
        final SchemaPath key = SchemaPath.getCompoundPath(pathSegments);
        return key.getAsUnescapedPath();
    }
    
    protected static boolean supportsCatalogs(final JdbcDremioSqlDialect dialect, final DatabaseMetaData metaData) throws SQLException {
        if (dialect.supportsCatalogs() == DremioSqlDialect.ContainerSupport.AUTO_DETECT) {
            return !Strings.isNullOrEmpty(metaData.getCatalogTerm());
        }
        return dialect.supportsCatalogs() == DremioSqlDialect.ContainerSupport.SUPPORTED;
    }
    
    protected static boolean supportsCatalogsWithoutSchemas(final JdbcDremioSqlDialect dialect, final DatabaseMetaData metaData) throws SQLException {
        return supportsCatalogs(dialect, metaData) && !supportsSchemas(dialect, metaData);
    }
    
    protected static boolean supportsSchemas(final JdbcDremioSqlDialect dialect, final DatabaseMetaData metaData) throws SQLException {
        if (dialect.supportsSchemas() == DremioSqlDialect.ContainerSupport.AUTO_DETECT) {
            return !Strings.isNullOrEmpty(metaData.getSchemaTerm());
        }
        return dialect.supportsSchemas() == DremioSqlDialect.ContainerSupport.SUPPORTED;
    }
    
    protected static boolean supportsSchemasWithoutCatalogs(final JdbcDremioSqlDialect dialect, final DatabaseMetaData metaData) throws SQLException {
        return supportsSchemas(dialect, metaData) && !supportsCatalogs(dialect, metaData);
    }
    
    private List<String> getCatalogsOrSchemas() {
        if (this.config.showOnlyConnDatabase() && this.config.getDatabase() != null) {
            return (List<String>)ImmutableList.of((Object)this.config.getDatabase());
        }
        try {
            final Connection connection = this.dataSource.getConnection();
            Throwable x0 = null;
            try {
                final DatabaseMetaData metaData = connection.getMetaData();
                if (supportsSchemasWithoutCatalogs(this.config.getDialect(), metaData)) {
                    return getSchemas(metaData, null, this.config, new ArrayList<String>());
                }
                final ImmutableList.Builder<String> catalogs = (ImmutableList.Builder<String>)ImmutableList.builder();
                JdbcSchemaFetcher.logger.debug("Getting catalogs from JDBC source {}", (Object)this.storagePluginName);
                final ResultSet getCatalogsResultSet = metaData.getCatalogs();
                Throwable x2 = null;
                try {
                    while (getCatalogsResultSet.next()) {
                        catalogs.add((Object)getCatalogsResultSet.getString(1));
                    }
                }
                catch (Throwable t) {
                    x2 = t;
                    throw t;
                }
                finally {
                    if (getCatalogsResultSet != null) {
                        $closeResource(x2, getCatalogsResultSet);
                    }
                }
                return (List<String>)catalogs.build();
            }
            catch (Throwable t2) {
                x0 = t2;
                throw t2;
            }
            finally {
                if (connection != null) {
                    $closeResource(x0, connection);
                }
            }
        }
        catch (SQLException e) {
            JdbcSchemaFetcher.logger.error("Error getting catalogs", (Throwable)e);
            throw new RuntimeException(StoragePluginUtils.generateSourceErrorMessage(this.storagePluginName, "Exception while fetching catalog information."), e);
        }
    }
    
    private List<String> getSchemas(final String catalogName) {
        try {
            final Connection connection = this.dataSource.getConnection();
            Throwable x0 = null;
            try {
                return getSchemas(connection.getMetaData(), catalogName, this.config, new ArrayList<String>());
            }
            catch (Throwable t) {
                x0 = t;
                throw t;
            }
            finally {
                if (connection != null) {
                    $closeResource(x0, connection);
                }
            }
        }
        catch (SQLException e) {
            JdbcSchemaFetcher.logger.error("Error getting schemas", (Throwable)e);
            throw new RuntimeException(StoragePluginUtils.generateSourceErrorMessage(this.storagePluginName, "Exception while fetching schema information."), e);
        }
    }
    
    private Optional<DatasetHandle> getDatasetHandleViaGetTables(final List<String> tableSchemaPath, final Connection connection) throws SQLException {
        final DatabaseMetaData metaData = connection.getMetaData();
        final FilterDescriptor filter = new FilterDescriptor(tableSchemaPath, supportsCatalogsWithoutSchemas(this.config.getDialect(), metaData));
        final ResultSet tablesResult = metaData.getTables(filter.getCatalogName(), filter.getSchemaName(), filter.getTableName(), null);
        Throwable x0 = null;
        try {
            while (tablesResult.next()) {
                final String currSchema = tablesResult.getString(2);
                if (!Strings.isNullOrEmpty(currSchema) && this.config.getHiddenSchemas().contains(currSchema)) {
                    continue;
                }
                final ImmutableList.Builder<String> pathBuilder = (ImmutableList.Builder<String>)ImmutableList.builder();
                pathBuilder.add((Object)this.storagePluginName);
                final String currCatalog = tablesResult.getString(1);
                if (!Strings.isNullOrEmpty(currCatalog)) {
                    pathBuilder.add((Object)currCatalog);
                }
                if (!Strings.isNullOrEmpty(currSchema)) {
                    pathBuilder.add((Object)currSchema);
                }
                pathBuilder.add((Object)tablesResult.getString(3));
                return (Optional<DatasetHandle>)Optional.of(new JdbcDatasetHandle(new EntityPath((List)pathBuilder.build())));
            }
        }
        catch (Throwable t) {
            x0 = t;
            throw t;
        }
        finally {
            if (tablesResult != null) {
                $closeResource(x0, tablesResult);
            }
        }
        return Optional.empty();
    }
    
    private Optional<DatasetHandle> getTableHandleViaPrepare(final List<String> tableSchemaPath, final Connection connection) throws SQLException {
        final DatabaseMetaData metaData = connection.getMetaData();
        final List<String> trimmedList = tableSchemaPath.subList(1, tableSchemaPath.size());
        final PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + this.getQuotedPath(trimmedList));
        Throwable x0 = null;
        try {
            final ResultSetMetaData preparedMetadata = statement.getMetaData();
            if (preparedMetadata.getColumnCount() <= 0) {
                JdbcSchemaFetcher.logger.debug("Table has no columns, query is in invalid");
                return Optional.empty();
            }
            final ImmutableList.Builder<String> pathBuilder = (ImmutableList.Builder<String>)ImmutableList.builder();
            pathBuilder.add((Object)this.storagePluginName);
            if (supportsCatalogs(this.config.getDialect(), metaData)) {
                final String catalog = preparedMetadata.getCatalogName(1);
                if (!Strings.isNullOrEmpty(catalog)) {
                    pathBuilder.add((Object)catalog);
                }
            }
            if (supportsSchemas(this.config.getDialect(), metaData)) {
                final String schema = preparedMetadata.getSchemaName(1);
                if (!Strings.isNullOrEmpty(schema)) {
                    pathBuilder.add((Object)schema);
                }
            }
            final String table = preparedMetadata.getTableName(1);
            if (Strings.isNullOrEmpty(table)) {
                JdbcSchemaFetcher.logger.info("Unable to get table handle for {} via prepare, falling back to getTables.", (Object)this.getQuotedPath(tableSchemaPath));
                return this.getDatasetHandleViaGetTables(tableSchemaPath, connection);
            }
            pathBuilder.add((Object)table);
            return (Optional<DatasetHandle>)Optional.of(new JdbcDatasetHandle(new EntityPath((List)pathBuilder.build())));
        }
        catch (Throwable t) {
            x0 = t;
            throw t;
        }
        finally {
            if (statement != null) {
                $closeResource(x0, statement);
            }
        }
    }
    
    private static /* synthetic */ void $closeResource(final Throwable x0, final AutoCloseable x1) {
        if (x0 != null) {
            try {
                x1.close();
            }
            catch (Throwable t) {
                x0.addSuppressed(t);
            }
        }
        else {
            x1.close();
        }
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcSchemaFetcher.class);
        PERIOD_JOINER = Joiner.on(".");
    }
    
    protected static class JdbcDatasetHandle implements DatasetHandle
    {
        private final EntityPath entityPath;
        
        public JdbcDatasetHandle(final EntityPath entityPath) {
            this.entityPath = entityPath;
        }
        
        public EntityPath getDatasetPath() {
            return this.entityPath;
        }
        
        public List<String> getIdentifiers() {
            final List<String> components = (List<String>)this.entityPath.getComponents();
            return components.subList(1, components.size());
        }
    }
    
    protected static class FilterDescriptor
    {
        private final String catalogName;
        private final String schemaName;
        private final String tableName;
        
        public FilterDescriptor(final List<String> tableSchemaPath, final boolean hasCatalogsWithoutSchemas) {
            if (tableSchemaPath.size() == 1 && tableSchemaPath.get(0).equals("%")) {
                this.catalogName = null;
                this.schemaName = null;
                this.tableName = "%";
                return;
            }
            assert tableSchemaPath.size() > 1;
            final List<String> tableIdentifier = tableSchemaPath.subList(1, tableSchemaPath.size());
            if (tableIdentifier.size() == 1) {
                this.catalogName = "";
                this.schemaName = "";
                this.tableName = tableIdentifier.get(0);
            }
            else if (tableIdentifier.size() == 2) {
                this.catalogName = (hasCatalogsWithoutSchemas ? tableIdentifier.get(0) : "");
                this.schemaName = (hasCatalogsWithoutSchemas ? "" : tableIdentifier.get(0));
                this.tableName = tableIdentifier.get(1);
            }
            else {
                assert tableIdentifier.size() == 3;
                this.catalogName = tableIdentifier.get(0);
                this.schemaName = tableIdentifier.get(1);
                this.tableName = tableIdentifier.get(2);
            }
        }
        
        public String getCatalogName() {
            return this.catalogName;
        }
        
        public String getSchemaName() {
            return this.schemaName;
        }
        
        public String getTableName() {
            return this.tableName;
        }
    }
    
    protected static class JdbcIteratorListing<T extends java.util.Iterator> implements DatasetHandleListing
    {
        private final T iterator;
        
        public JdbcIteratorListing(final T iterator) {
            this.iterator = (Iterator)iterator;
        }
        
        public Iterator<DatasetHandle> iterator() {
            return (Iterator<DatasetHandle>)this.iterator;
        }
        
        public void close() {
            try {
                ((AutoCloseable)this.iterator).close();
            }
            catch (Exception e) {
                JdbcSchemaFetcher.logger.warn("Error closing iterator when listing JDBC datasets.", (Throwable)e);
            }
        }
    }
    
    private static class JdbcDatasetMetadataIterable extends AbstractIterator<DatasetHandle> implements AutoCloseable
    {
        private final String storagePluginName;
        private final JdbcStoragePlugin.Config config;
        private Connection connection;
        private DatabaseMetaData metaData;
        private String[] tableTypes;
        private boolean supportsCatalogs;
        private boolean hasConstantSchema;
        private boolean hasErrorDuringRetrieval;
        private final List<String> failedCatalogOrSchema;
        private Iterator<String> catalogs;
        private String currentCatalog;
        private Iterator<String> schemas;
        private String currentSchema;
        private ResultSet tablesResult;
        
        JdbcDatasetMetadataIterable(final String storagePluginName, final DataSource dataSource, final JdbcStoragePlugin.Config config) {
            this.failedCatalogOrSchema = new ArrayList<String>();
            this.currentCatalog = null;
            this.schemas = null;
            this.currentSchema = null;
            this.tablesResult = null;
            this.storagePluginName = storagePluginName;
            this.config = config;
            this.hasErrorDuringRetrieval = false;
            try {
                this.connection = dataSource.getConnection();
                this.metaData = this.connection.getMetaData();
                this.supportsCatalogs = JdbcSchemaFetcher.supportsCatalogs(config.getDialect(), this.metaData);
                if (config.getDatabase() != null && config.showOnlyConnDatabase()) {
                    if (this.supportsCatalogs) {
                        this.catalogs = (Iterator<String>)ImmutableList.of((Object)config.getDatabase()).iterator();
                    }
                    else if (JdbcSchemaFetcher.supportsSchemasWithoutCatalogs(config.getDialect(), this.metaData)) {
                        this.currentSchema = config.getDatabase();
                    }
                }
                if (null == this.catalogs) {
                    if (this.supportsCatalogs) {
                        this.catalogs = this.getCatalogs(this.metaData).iterator();
                        if (!this.catalogs.hasNext()) {
                            this.catalogs = Collections.singleton("").iterator();
                        }
                    }
                    else {
                        this.catalogs = Collections.singleton((String)null).iterator();
                    }
                }
                this.hasConstantSchema = (null != this.currentSchema || !JdbcSchemaFetcher.supportsSchemas(config.getDialect(), this.metaData));
                this.tableTypes = this.getTableTypes(this.metaData);
            }
            catch (SQLException e) {
                JdbcSchemaFetcher.logger.error(String.format("Error retrieving all tables for %s", storagePluginName), (Throwable)e);
                this.catalogs = Collections.emptyIterator();
            }
        }
        
        protected DatasetHandle computeNext() {
            while (true) {
                if (this.supportsCatalogs && this.currentCatalog == null) {
                    if (!this.catalogs.hasNext()) {
                        return this.end();
                    }
                    this.currentCatalog = this.catalogs.next();
                    this.tablesResult = null;
                    if (!this.hasConstantSchema) {
                        this.currentSchema = null;
                        this.schemas = null;
                    }
                }
                if (!this.hasConstantSchema && this.currentSchema == null) {
                    if (this.schemas == null) {
                        final List<String> schemaFailures = new ArrayList<String>();
                        this.schemas = JdbcSchemaFetcher.getSchemas(this.metaData, this.currentCatalog, this.config, schemaFailures).iterator();
                        this.hasErrorDuringRetrieval |= !schemaFailures.isEmpty();
                        if (this.hasErrorDuringRetrieval && JdbcSchemaFetcher.logger.isDebugEnabled()) {
                            this.failedCatalogOrSchema.addAll(schemaFailures);
                        }
                    }
                    if (!this.schemas.hasNext()) {
                        if (!this.supportsCatalogs) {
                            return this.end();
                        }
                        this.currentCatalog = null;
                        continue;
                    }
                    else {
                        this.currentSchema = this.schemas.next();
                        this.tablesResult = null;
                    }
                }
                try {
                    if (this.tablesResult == null) {
                        try {
                            this.tablesResult = this.metaData.getTables(this.currentCatalog, this.currentSchema, null, this.tableTypes);
                        }
                        catch (SQLException e1) {
                            this.hasErrorDuringRetrieval = true;
                            if (JdbcSchemaFetcher.logger.isDebugEnabled()) {
                                this.failedCatalogOrSchema.add(JdbcSchemaFetcher.getJoinedSchema(this.currentCatalog, this.currentSchema, null));
                            }
                            if (!this.hasConstantSchema) {
                                this.currentSchema = null;
                                continue;
                            }
                            if (this.supportsCatalogs) {
                                this.currentCatalog = null;
                                continue;
                            }
                            throw e1;
                        }
                    }
                    if (this.tablesResult.next()) {
                        final List<String> path = new ArrayList<String>(4);
                        path.add(this.storagePluginName);
                        final String currCatalog = this.tablesResult.getString(1);
                        if (!Strings.isNullOrEmpty(currCatalog)) {
                            path.add(currCatalog);
                        }
                        final String currSchema = this.tablesResult.getString(2);
                        if (!Strings.isNullOrEmpty(currSchema)) {
                            path.add(currSchema);
                        }
                        path.add(this.tablesResult.getString(3));
                        return (DatasetHandle)new JdbcDatasetHandle(new EntityPath((List)path));
                    }
                    this.tablesResult.close();
                    if (this.hasConstantSchema) {
                        if (!this.supportsCatalogs) {
                            return this.end();
                        }
                        this.currentCatalog = null;
                    }
                    else {
                        this.currentSchema = null;
                    }
                }
                catch (SQLException e2) {
                    JdbcSchemaFetcher.logger.error(String.format("Error listing datasets for '%s'", this.storagePluginName), (Throwable)e2);
                    return (DatasetHandle)this.endOfData();
                }
            }
        }
        
        private DatasetHandle end() {
            JdbcSchemaFetcher.logger.debug("Done fetching all schema and tables for '{}'.", (Object)this.storagePluginName);
            if (this.hasErrorDuringRetrieval) {
                if (JdbcSchemaFetcher.logger.isDebugEnabled()) {
                    JdbcSchemaFetcher.logger.debug("Failed to fetch schema for {}.", (Object)this.failedCatalogOrSchema);
                }
                else {
                    JdbcSchemaFetcher.logger.warn("Failed to fetch some tables, for more information enable debug logging.");
                }
            }
            return (DatasetHandle)this.endOfData();
        }
        
        private List<String> getCatalogs(final DatabaseMetaData metaData) {
            final ImmutableList.Builder<String> catalogs = (ImmutableList.Builder<String>)ImmutableList.builder();
            try {
                final ResultSet getCatalogsResultSet = metaData.getCatalogs();
                Throwable x0 = null;
                try {
                    while (getCatalogsResultSet.next()) {
                        catalogs.add((Object)getCatalogsResultSet.getString(1));
                    }
                }
                catch (Throwable t) {
                    x0 = t;
                    throw t;
                }
                finally {
                    if (getCatalogsResultSet != null) {
                        $closeResource(x0, getCatalogsResultSet);
                    }
                }
            }
            catch (SQLException e) {
                JdbcSchemaFetcher.logger.error(String.format("Failed to get catalogs for plugin '%s'.", this.storagePluginName), (Throwable)e);
            }
            return (List<String>)catalogs.build();
        }
        
        private String[] getTableTypes(final DatabaseMetaData metaData) {
            if (this.tableTypes != null) {
                return this.tableTypes;
            }
            try {
                final ResultSet typesResult = metaData.getTableTypes();
                Throwable x0 = null;
                try {
                    final List<String> types = (List<String>)Lists.newArrayList();
                    while (typesResult.next()) {
                        final String type = typesResult.getString(1).trim();
                        if (!this.config.getHiddenTableTypes().contains(type)) {
                            types.add(type);
                        }
                    }
                    if (types.isEmpty()) {
                        return null;
                    }
                    return types.toArray(new String[0]);
                }
                catch (Throwable t) {
                    x0 = t;
                    throw t;
                }
                finally {
                    if (typesResult != null) {
                        $closeResource(x0, typesResult);
                    }
                }
            }
            catch (SQLException e) {
                JdbcSchemaFetcher.logger.warn("Unable to retrieve list of table types.", (Throwable)e);
                return null;
            }
        }
        
        public void close() throws Exception {
            AutoCloseables.close(new AutoCloseable[] { this.tablesResult, this.connection });
        }
        
        private static /* synthetic */ void $closeResource(final Throwable x0, final AutoCloseable x1) {
            if (x0 != null) {
                try {
                    x1.close();
                }
                catch (Throwable t) {
                    x0.addSuppressed(t);
                }
            }
            else {
                x1.close();
            }
        }
    }
}
