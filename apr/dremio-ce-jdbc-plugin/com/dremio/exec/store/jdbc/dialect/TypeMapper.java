package com.dremio.exec.store.jdbc.dialect;

import com.google.common.collect.*;
import java.util.*;
import java.util.stream.*;
import org.apache.calcite.sql.validate.*;
import java.sql.*;
import java.lang.reflect.*;
import com.dremio.common.expression.*;
import org.slf4j.*;

public abstract class TypeMapper
{
    public static final String TO_DATE_NAME = "TO_DATE";
    public static final String DATE_TRUNC_NAME = "DATE_TRUNC";
    private static final Logger logger;
    protected final boolean useDecimalToDoubleMapping;
    
    public TypeMapper(final boolean useDecimalToDoubleMapping) {
        this.useDecimalToDoubleMapping = useDecimalToDoubleMapping;
    }
    
    public final List<JdbcToFieldMapping> mapJdbcToArrowFields(final UnrecognizedTypeMarker unrecognizedTypeCallback, final AddPropertyCallback addColumnPropertyCallback, final Connection connection, final String catalog, final String schema, final String table, final boolean mapSkippedColumnsAsNullType) throws SQLException {
        final List<SourceTypeDescriptor> sourceTypes = this.convertGetColumnsCallToSourceTypeDescriptors(addColumnPropertyCallback, connection, new TableIdentifier(catalog, schema, table));
        return this.mapSourceToArrowFields(this.handleUnknownType(unrecognizedTypeCallback), addColumnPropertyCallback, sourceTypes, mapSkippedColumnsAsNullType);
    }
    
    public final List<JdbcToFieldMapping> mapJdbcToArrowFields(final UnrecognizedTypeMarker unrecognizedTypeCallback, final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final ResultSetMetaData metaData, final Set<String> unsupportedColumns, final boolean mapSkippedColumnsAsNullType) throws SQLException {
        return this.mapJdbcToArrowFields(unrecognizedTypeCallback, addColumnPropertyCallback, invalidMetaDataCallback, connection, null, null, null, metaData, unsupportedColumns, true, mapSkippedColumnsAsNullType);
    }
    
    public final List<JdbcToFieldMapping> mapJdbcToArrowFields(final UnrecognizedTypeMarker unrecognizedTypeCallback, final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final String catalog, final String schema, final String table, final ResultSetMetaData metaData, final Set<String> unsupportedColumns, final boolean changeColumnCasing, final boolean mapSkippedColumnsAsNullType) throws SQLException {
        final List<SourceTypeDescriptor> sourceTypes = this.convertResultSetMetaDataToSourceTypeDescriptors(addColumnPropertyCallback, invalidMetaDataCallback, connection, catalog, schema, table, metaData, unsupportedColumns, changeColumnCasing);
        return this.mapSourceToArrowFields(this.handleUnknownType(unrecognizedTypeCallback), addColumnPropertyCallback, sourceTypes, mapSkippedColumnsAsNullType);
    }
    
    protected abstract List<JdbcToFieldMapping> mapSourceToArrowFields(final UnrecognizedTypeCallback p0, final AddPropertyCallback p1, final List<SourceTypeDescriptor> p2, final boolean p3);
    
    private List<SourceTypeDescriptor> convertResultSetMetaDataToSourceTypeDescriptors(final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final String catalog, final String schema, final String table, final ResultSetMetaData metaData, Set<String> unsupportedColumns, final boolean changeColumnCasing) throws SQLException {
        TypeMapper.logger.debug("Getting column types from metadata for table with catalogName=[{}], schemaName=[{}], tableName=[{}].", new Object[] { catalog, schema, table });
        final ImmutableList.Builder<SourceTypeDescriptor> builder = (ImmutableList.Builder<SourceTypeDescriptor>)ImmutableList.builder();
        final Set<String> usedNames = new HashSet<String>();
        unsupportedColumns = ((unsupportedColumns == null) ? new HashSet<String>() : (changeColumnCasing ? unsupportedColumns.stream().map(skippedColumn -> skippedColumn.toLowerCase(Locale.ROOT)).collect((Collector<? super Object, ?, Set<String>>)Collectors.toSet()) : unsupportedColumns));
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            String columnLabel = metaData.getColumnLabel(i);
            if (changeColumnCasing) {
                columnLabel = columnLabel.toLowerCase(Locale.ROOT);
            }
            if (!unsupportedColumns.contains(columnLabel)) {
                columnLabel = SqlValidatorUtil.uniquify(columnLabel, (Set)usedNames, SqlValidatorUtil.EXPR_SUGGESTER);
                usedNames.add(columnLabel);
                builder.add((Object)this.createTypeDescriptor(addColumnPropertyCallback, invalidMetaDataCallback, connection, new TableIdentifier(catalog, schema, table), metaData, columnLabel, i));
            }
        }
        return (List<SourceTypeDescriptor>)builder.build();
    }
    
    protected SourceTypeDescriptor createTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final TableIdentifier table, final ResultSetMetaData metaData, final String columnLabel, final int colIndex) throws SQLException {
        return new SourceTypeDescriptor(columnLabel, metaData.getColumnType(colIndex), metaData.getColumnTypeName(colIndex), colIndex, metaData.getPrecision(colIndex), metaData.getScale(colIndex));
    }
    
    protected SourceTypeDescriptor createTableTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final Connection connection, final String columnName, final int sourceJdbcType, final String typeString, final TableIdentifier table, final int colIndex, final int precision, final int scale) {
        return new TableSourceTypeDescriptor(columnName, sourceJdbcType, typeString, table.catalog, table.schema, table.table, colIndex, precision, scale);
    }
    
    private List<SourceTypeDescriptor> convertGetColumnsCallToSourceTypeDescriptors(final AddPropertyCallback addColumnPropertyCallback, final Connection connection, final TableIdentifier table) throws SQLException {
        TypeMapper.logger.debug("Getting column types from getColumns for tables with catalogName=[{}], schemaName=[{}], tableName=[{}].", new Object[] { table.catalog, table.schema, table.table });
        final DatabaseMetaData metaData = connection.getMetaData();
        try (final ResultSet getColumnsResultSet = metaData.getColumns(table.catalog, table.schema, table.table, "%")) {
            final ImmutableList.Builder<SourceTypeDescriptor> builder = (ImmutableList.Builder<SourceTypeDescriptor>)ImmutableList.builder();
            int jdbcOrdinal = 0;
            String newCatalog = "";
            String newSchema = "";
            String newTable = "";
            while (getColumnsResultSet.next()) {
                if (jdbcOrdinal == 0) {
                    newCatalog = getColumnsResultSet.getString(1);
                    newSchema = getColumnsResultSet.getString(2);
                    newTable = getColumnsResultSet.getString(3);
                }
                ++jdbcOrdinal;
                final String columnName = getColumnsResultSet.getString(4);
                final int sourceJdbcType = getColumnsResultSet.getInt(5);
                final String typeString = getColumnsResultSet.getString(6);
                int precision;
                int scale;
                if (sourceJdbcType == 3 || sourceJdbcType == 2) {
                    precision = getColumnsResultSet.getInt(7);
                    scale = getColumnsResultSet.getInt(9);
                }
                else {
                    precision = 0;
                    scale = 0;
                }
                builder.add((Object)this.createTableTypeDescriptor(addColumnPropertyCallback, connection, columnName, sourceJdbcType, typeString, new TableIdentifier(newCatalog, newSchema, newTable), jdbcOrdinal, precision, scale));
            }
            return (List<SourceTypeDescriptor>)builder.build();
        }
    }
    
    public static String nameFromType(final int javaSqlType) {
        try {
            for (final Field f : Types.class.getFields()) {
                if (Modifier.isStatic(f.getModifiers()) && f.getType() == Integer.TYPE && f.getInt(null) == javaSqlType) {
                    return f.getName();
                }
            }
        }
        catch (IllegalArgumentException ex) {}
        catch (IllegalAccessException ex2) {}
        return Integer.toString(javaSqlType);
    }
    
    private UnrecognizedTypeCallback handleUnknownType(final UnrecognizedTypeMarker unrecognizedTypeCallback) {
        int prec;
        int scale;
        return (sourceTypeDescriptor, shouldSkip) -> {
            if (unrecognizedTypeCallback != null) {
                unrecognizedTypeCallback.markUnrecognized(sourceTypeDescriptor, shouldSkip);
            }
            switch (sourceTypeDescriptor.getReportedJdbcType()) {
                case -7:
                case 16: {
                    return CompleteType.BIT;
                }
                case -6:
                case 4:
                case 5: {
                    return CompleteType.INT;
                }
                case -8:
                case -5: {
                    return CompleteType.BIGINT;
                }
                case 6:
                case 7: {
                    return CompleteType.FLOAT;
                }
                case 8: {
                    return CompleteType.DOUBLE;
                }
                case 2:
                case 3: {
                    prec = sourceTypeDescriptor.getPrecision();
                    scale = sourceTypeDescriptor.getScale();
                    if (prec > 38) {
                        if (scale > 6) {
                            scale = Math.max(6, scale - (prec - 38));
                        }
                        prec = 38;
                    }
                    return CompleteType.fromDecimalPrecisionScale(prec, scale);
                }
                case 91: {
                    return CompleteType.DATE;
                }
                case 92: {
                    return CompleteType.TIME;
                }
                case 93: {
                    return CompleteType.TIMESTAMP;
                }
                case -4:
                case -3:
                case -2:
                case 2004: {
                    return CompleteType.VARBINARY;
                }
                default: {
                    return CompleteType.VARCHAR;
                }
            }
        };
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)TypeMapper.class);
    }
    
    public class TableIdentifier
    {
        public final String catalog;
        public final String schema;
        public final String table;
        
        public TableIdentifier(final String catalog, final String schema, final String table) {
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
        }
    }
    
    @FunctionalInterface
    public interface InvalidMetaDataCallback
    {
        void throwOnInvalidMetaData(final String p0);
    }
    
    @FunctionalInterface
    public interface UnrecognizedTypeCallback
    {
        CompleteType mark(final SourceTypeDescriptor p0, final boolean p1);
    }
    
    @FunctionalInterface
    public interface UnrecognizedTypeMarker
    {
        void markUnrecognized(final SourceTypeDescriptor p0, final boolean p1);
    }
    
    @FunctionalInterface
    public interface AddPropertyCallback
    {
        void addProperty(final String p0, final String p1, final String p2);
    }
}
