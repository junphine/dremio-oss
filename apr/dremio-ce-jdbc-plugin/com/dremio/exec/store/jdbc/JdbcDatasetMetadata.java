package com.dremio.exec.store.jdbc;

import com.dremio.exec.store.jdbc.proto.*;
import org.apache.arrow.vector.types.pojo.*;
import java.util.function.*;
import java.util.stream.*;
import com.dremio.connector.metadata.*;
import com.google.common.base.*;
import java.util.concurrent.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.store.*;
import java.sql.*;
import com.google.common.collect.*;
import java.util.*;
import com.dremio.exec.store.jdbc.dialect.*;
import java.io.*;
import org.slf4j.*;
import com.dremio.exec.planner.cost.*;

public class JdbcDatasetMetadata implements DatasetMetadata, PartitionChunkListing
{
    private static final Logger logger;
    public static final String UNPUSHABLE_KEY = "unpushable";
    public static final String TYPENAME_KEY = "sourceTypeName";
    public static final String EXPLICIT_CAST_KEY = "explicitCast";
    private static final DatasetStats JDBC_STATS;
    private final JdbcSchemaFetcher fetcher;
    private final JdbcSchemaFetcher.JdbcDatasetHandle handle;
    private List<String> skippedColumns;
    private List<JdbcReaderProto.ColumnProperties> columnProperties;
    private List<JdbcToFieldMapping> jdbcToFieldMappings;
    
    JdbcDatasetMetadata(final JdbcSchemaFetcher fetcher, final JdbcSchemaFetcher.JdbcDatasetHandle handle) {
        this.fetcher = fetcher;
        this.handle = handle;
    }
    
    public DatasetStats getDatasetStats() {
        return JdbcDatasetMetadata.JDBC_STATS;
    }
    
    public BytesOutput getExtraInfo() {
        this.buildIfNecessary();
        return os -> os.write(JdbcReaderProto.JdbcTableXattr.newBuilder().addAllSkippedColumns(this.skippedColumns).addAllColumnProperties(this.columnProperties).build().toByteArray());
    }
    
    public Schema getRecordSchema() {
        this.buildIfNecessary();
        return new Schema((Iterable)this.jdbcToFieldMappings.stream().map((Function<? super Object, ?>)JdbcToFieldMapping::getField).collect((Collector<? super Object, ?, List<? super Object>>)Collectors.toList()));
    }
    
    public Iterator<? extends PartitionChunk> iterator() {
        return Collections.singleton(PartitionChunk.of(new DatasetSplit[] { DatasetSplit.of(Long.MAX_VALUE, this.fetcher.getRowCount(this.handle)) })).iterator();
    }
    
    private void buildIfNecessary() {
        if (this.jdbcToFieldMappings == null) {
            final Stopwatch watch = Stopwatch.createStarted();
            if (this.fetcher.usePrepareForColumnMetadata()) {
                this.prepareColumnMetadata();
            }
            else {
                this.apiColumnMetadata();
            }
            JdbcDatasetMetadata.logger.info("Took {} ms to get column metadata for {}", (Object)watch.elapsed(TimeUnit.MILLISECONDS), (Object)this.handle.getDatasetPath());
        }
    }
    
    private void apiColumnMetadata() {
        this.getColumnMetadata((connection, filters, skippedColumnBuilder, properties) -> this.fetcher.config.getDialect().getDataTypeMapper().mapJdbcToArrowFields((sourceTypeDescriptor, shouldSkip) -> this.handleUnpushableColumn(skippedColumnBuilder, properties, sourceTypeDescriptor, shouldSkip), (colName, propName, propValue) -> this.addColumnProperty(properties, colName, propName, propValue), connection, filters.getCatalogName(), filters.getSchemaName(), filters.getTableName(), false));
    }
    
    private void prepareColumnMetadata() {
        final String quotedPath;
        final PreparedStatement statement;
        final Throwable x0;
        this.getColumnMetadata((connection, filters, skippedColumnBuilder, properties) -> {
            quotedPath = this.fetcher.getQuotedPath(this.handle.getIdentifiers());
            statement = connection.prepareStatement("SELECT * FROM " + quotedPath);
            try {
                return this.fetcher.config.getDialect().getDataTypeMapper().mapJdbcToArrowFields((sourceTypeDescriptor, shouldSkip) -> this.handleUnpushableColumn(skippedColumnBuilder, properties, sourceTypeDescriptor, shouldSkip), (colName, propName, propValue) -> this.addColumnProperty(properties, colName, propName, propValue), message -> {
                    throw new IllegalArgumentException(message);
                }, connection, filters.getCatalogName(), filters.getSchemaName(), filters.getTableName(), statement.getMetaData(), null, false, false);
            }
            catch (Throwable t) {
                throw t;
            }
            finally {
                if (statement != null) {
                    $closeResource(x0, statement);
                }
            }
        });
    }
    
    private void getColumnMetadata(final MapFunction mapFields) {
        try {
            final Connection connection = this.fetcher.dataSource.getConnection();
            Throwable x0 = null;
            try {
                final DatabaseMetaData metaData = connection.getMetaData();
                final JdbcSchemaFetcher.FilterDescriptor filters = new JdbcSchemaFetcher.FilterDescriptor(this.handle.getDatasetPath().getComponents(), JdbcSchemaFetcher.supportsCatalogsWithoutSchemas(this.fetcher.config.getDialect(), metaData));
                final ImmutableList.Builder<String> skippedColumnBuilder = (ImmutableList.Builder<String>)ImmutableList.builder();
                final ListMultimap<String, JdbcReaderProto.ColumnProperty> properties = (ListMultimap<String, JdbcReaderProto.ColumnProperty>)ArrayListMultimap.create();
                this.jdbcToFieldMappings = mapFields.map(connection, filters, skippedColumnBuilder, properties);
                final ImmutableList.Builder<JdbcReaderProto.ColumnProperties> columnPropertiesListBuilder = (ImmutableList.Builder<JdbcReaderProto.ColumnProperties>)ImmutableList.builder();
                for (final String colName : properties.keys()) {
                    columnPropertiesListBuilder.add((Object)JdbcReaderProto.ColumnProperties.newBuilder().setColumnName(colName).addAllProperties(properties.get((Object)colName)).build());
                }
                this.columnProperties = (List<JdbcReaderProto.ColumnProperties>)columnPropertiesListBuilder.build();
                this.skippedColumns = (List<String>)skippedColumnBuilder.build();
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
            throw StoragePluginUtils.message(UserException.dataReadError((Throwable)e), this.fetcher.storagePluginName, "Failed getting columns for %s.", new Object[] { this.fetcher.getQuotedPath(this.handle.getDatasetPath().getComponents()) }).build(JdbcDatasetMetadata.logger);
        }
    }
    
    private void handleUnpushableColumn(final ImmutableList.Builder<String> skippedColumnBuilder, final ListMultimap<String, JdbcReaderProto.ColumnProperty> properties, final SourceTypeDescriptor sourceTypeDescriptor, final Boolean shouldSkip) {
        if (shouldSkip) {
            this.addSkippedColumn(skippedColumnBuilder, sourceTypeDescriptor);
        }
        else {
            this.setColumnNotPushable(properties, sourceTypeDescriptor);
        }
    }
    
    private void addSkippedColumn(final ImmutableList.Builder<String> skippedColumnBuilder, final SourceTypeDescriptor sourceTypeDescriptor) {
        skippedColumnBuilder.add((Object)sourceTypeDescriptor.getFieldName().toLowerCase(Locale.ROOT));
        this.warnUnsupportedColumnType(sourceTypeDescriptor);
    }
    
    private void setColumnNotPushable(final ListMultimap<String, JdbcReaderProto.ColumnProperty> columnProperties, final SourceTypeDescriptor mapping) {
        this.addColumnProperty(columnProperties, mapping.getFieldName(), "unpushable", Boolean.TRUE.toString());
    }
    
    private void addColumnProperty(final ListMultimap<String, JdbcReaderProto.ColumnProperty> columnProperties, final String colName, final String key, final String value) {
        final JdbcReaderProto.ColumnProperty colProperty = JdbcReaderProto.ColumnProperty.newBuilder().setKey(key).setValue(value).build();
        columnProperties.put((Object)colName.toLowerCase(Locale.ROOT), (Object)colProperty);
    }
    
    protected void warnUnsupportedColumnType(final SourceTypeDescriptor type) {
        final TableSourceTypeDescriptor tableDescriptor = type.unwrap(TableSourceTypeDescriptor.class);
        String columnName;
        if (tableDescriptor != null) {
            columnName = String.format("%s.%s.%s.%s", tableDescriptor.getCatalog(), tableDescriptor.getSchema(), tableDescriptor.getTable(), type.getFieldName());
        }
        else {
            columnName = type.getFieldName();
        }
        JdbcDatasetMetadata.logger.warn("A column you queried has a data type that is not currently supported by the JDBC storage plugin. The column's name was {}, its JDBC data type was {}, and the source column type was {}.", new Object[] { columnName, TypeMapper.nameFromType(type.getReportedJdbcType()), type.getDataSourceTypeName() });
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
        logger = LoggerFactory.getLogger((Class)JdbcDatasetMetadata.class);
        JDBC_STATS = DatasetStats.of(ScanCostFactor.JDBC.getFactor());
    }
    
    @FunctionalInterface
    private interface MapFunction
    {
        List<JdbcToFieldMapping> map(final Connection p0, final JdbcSchemaFetcher.FilterDescriptor p1, final ImmutableList.Builder<String> p2, final ListMultimap<String, JdbcReaderProto.ColumnProperty> p3) throws SQLException;
    }
}
