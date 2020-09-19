package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.store.jdbc.legacy.*;
import javax.sql.*;
import com.google.common.base.*;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import java.text.*;
import java.util.*;
import java.sql.*;
import org.apache.calcite.sql.type.*;

public final class PostgreSQLDialect extends ArpDialect
{
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final boolean DISABLE_PUSH_COLLATION;
    private final SqlCollation POSTGRES_BINARY_COLLATION;
    private final ArpTypeMapper typeMapper;
    
    public PostgreSQLDialect(final ArpYaml yaml) {
        super(yaml);
        this.POSTGRES_BINARY_COLLATION = new SqlCollation(SqlCollation.Coercibility.NONE) {
            private static final long serialVersionUID = 1L;
            
            public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
                writer.keyword("COLLATE");
                writer.keyword("\"C\"");
            }
        };
        this.typeMapper = new PostgreSQLTypeMapper(yaml);
    }
    
    protected boolean requiresAliasForFromItems() {
        return true;
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    @Override
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
            PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    @Override
    public TypeMapper getDataTypeMapper() {
        return this.typeMapper;
    }
    
    @Override
    public SqlNode getCastSpec(final RelDataType type) {
        switch (type.getSqlTypeName()) {
            case DOUBLE: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("DOUBLE PRECISION", SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO) {
                    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
                        writer.keyword("DOUBLE PRECISION");
                    }
                };
            }
            default: {
                return super.getCastSpec(type);
            }
        }
    }
    
    @Override
    public SqlNode emulateNullDirection(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return null;
    }
    
    @Override
    public PostgresRelToSqlConverter getConverter() {
        return new PostgresRelToSqlConverter(this);
    }
    
    @Override
    public ArpSchemaFetcher getSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        final String query = String.format("SELECT NULL, SCH, NME FROM (SELECT TABLEOWNER CAT, SCHEMANAME SCH, TABLENAME NME from pg_catalog.pg_tables UNION ALL SELECT VIEWOWNER CAT, SCHEMANAME SCH, VIEWNAME NME FROM pg_catalog.pg_views) t WHERE UPPER(SCH) NOT IN ('PG_CATALOG', '%s')", Joiner.on("','").join((Iterable)config.getHiddenSchemas()));
        return new PGSchemaFetcher(query, name, dataSource, timeout, config);
    }
    
    @Override
    public boolean supportsFetchOffsetInSetOperand() {
        return false;
    }
    
    public SqlCollation getDefaultCollation(final SqlKind kind) {
        if (PostgreSQLDialect.DISABLE_PUSH_COLLATION) {
            return null;
        }
        switch (kind) {
            case LITERAL:
            case IDENTIFIER: {
                return this.POSTGRES_BINARY_COLLATION;
            }
            default: {
                return null;
            }
        }
    }
    
    static {
        DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.postgres.push-collation.disable");
    }
    
    public static class PGSchemaFetcher extends ArpSchemaFetcher
    {
        public PGSchemaFetcher(final String query, final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
            super(query, name, dataSource, timeout, config);
        }
        
        @Override
        protected long getRowCount(final JdbcDatasetHandle handle) {
            final String sql = MessageFormat.format("SELECT reltuples::bigint AS EstimatedCount\nFROM pg_class\nWHERE  oid = {0}::regclass", this.config.getDialect().quoteStringLiteral(PGSchemaFetcher.PERIOD_JOINER.join((Iterable)handle.getIdentifiers())));
            final java.util.Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
            if (estimate.isPresent() && estimate.get() != 0L) {
                return estimate.get();
            }
            return super.getRowCount(handle);
        }
    }
    
    private static class PostgreSQLTypeMapper extends ArpTypeMapper
    {
        public PostgreSQLTypeMapper(final ArpYaml yaml) {
            super(yaml);
        }
        
        @Override
        protected SourceTypeDescriptor createTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final TableIdentifier table, final ResultSetMetaData metaData, final String columnLabel, final int colIndex) throws SQLException {
            final int colType = metaData.getColumnType(colIndex);
            int precision = metaData.getPrecision(colIndex);
            int scale = metaData.getScale(colIndex);
            if (colType == 2 && 0 == precision) {
                scale = 6;
                precision = 38;
                if (null != addColumnPropertyCallback) {
                    addColumnPropertyCallback.addProperty(columnLabel, "explicitCast", Boolean.TRUE.toString());
                }
            }
            return new SourceTypeDescriptor(columnLabel, colType, metaData.getColumnTypeName(colIndex), colIndex, precision, scale);
        }
        
        @Override
        protected SourceTypeDescriptor createTableTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final Connection connection, final String columnName, final int sourceJdbcType, final String typeString, final TableIdentifier table, final int colIndex, int precision, int scale) {
            if (precision == 131089) {
                scale = 6;
                precision = 38;
                if (null != addColumnPropertyCallback) {
                    addColumnPropertyCallback.addProperty(columnName, "explicitCast", Boolean.TRUE.toString());
                }
            }
            return new TableSourceTypeDescriptor(columnName, sourceJdbcType, typeString, table.catalog, table.schema, table.catalog, colIndex, precision, scale);
        }
    }
}
