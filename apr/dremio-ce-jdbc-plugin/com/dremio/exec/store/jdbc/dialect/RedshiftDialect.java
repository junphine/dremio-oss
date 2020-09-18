package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.*;
import org.apache.calcite.rel.type.*;
import java.util.*;
import org.apache.calcite.sql.fun.*;
import com.dremio.exec.store.jdbc.legacy.*;
import javax.sql.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.core.*;
import com.google.common.collect.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import com.google.common.base.*;
import java.sql.*;

public final class RedshiftDialect extends ArpDialect
{
    private final ArpTypeMapper typeMapper;
    
    public RedshiftDialect(final ArpYaml yaml) {
        super(yaml);
        this.typeMapper = new RedshiftTypeMapper(yaml);
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    @Override
    public boolean supportsFunction(final SqlOperator operator, final RelDataType type, final List<RelDataType> paramTypes) {
        return operator != SqlStdOperatorTable.DATETIME_MINUS && operator != SqlStdOperatorTable.DATETIME_PLUS && super.supportsFunction(operator, type, paramTypes);
    }
    
    @Override
    public RedshiftRelToSqlConverter getConverter() {
        return new RedshiftRelToSqlConverter(this);
    }
    
    @Override
    public ArpSchemaFetcher getSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        final String query = String.format("SELECT * FROM (SELECT CURRENT_DATABASE() CAT, SCHEMANAME SCH, TABLENAME NME from pg_catalog.pg_tables UNION ALL SELECT CURRENT_DATABASE() CAT, SCHEMANAME SCH, VIEWNAME NME FROM pg_catalog.pg_views) t WHERE UPPER(SCH) NOT IN ('PG_CATALOG', '%s')", Joiner.on("','").join((Iterable)config.getHiddenSchemas()));
        return new PostgreSQLDialect.PGSchemaFetcher(query, name, dataSource, timeout, config);
    }
    
    protected boolean requiresAliasForFromItems() {
        return true;
    }
    
    @Override
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
            RedshiftSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    @Override
    public SqlNode emulateNullDirection(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return null;
    }
    
    @Override
    public boolean supportsFetchOffsetInSetOperand() {
        return false;
    }
    
    public boolean useTimestampAddInsteadOfDatetimePlus() {
        return true;
    }
    
    public boolean removeDefaultWindowFrame(final RexOver over) {
        return SqlKind.AGGREGATE.contains(over.getAggOperator().getKind());
    }
    
    public boolean supportsOver(final RexOver over) {
        return over.getWindow() != null && over.getWindow().isRows();
    }
    
    public boolean supportsOver(final Window window) {
        if (window.groups.isEmpty()) {
            return false;
        }
        for (final Window.Group group : window.groups) {
            if (!group.isRows) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public boolean supportsLiteral(final CompleteType type) {
        return CompleteType.INTERVAL_DAY_SECONDS.equals((Object)type) || CompleteType.INTERVAL_YEAR_MONTHS.equals((Object)type) || super.supportsLiteral(type);
    }
    
    @Override
    public TypeMapper getDataTypeMapper() {
        return this.typeMapper;
    }
    
    private static class RedshiftTypeMapper extends ArpTypeMapper
    {
        public RedshiftTypeMapper(final ArpYaml yaml) {
            super(yaml);
        }
        
        @Override
        protected SourceTypeDescriptor createTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final TableIdentifier table, final ResultSetMetaData metaData, final String columnLabel, final int colIndex) throws SQLException {
            Preconditions.checkNotNull((Object)invalidMetaDataCallback);
            final int colType = metaData.getColumnType(colIndex);
            if ((colType == 2 || colType == 3) && metaData.getPrecision(colIndex) <= 0) {
                final String badPrecisionMessage = "Redshift server returned invalid metadata for a numeric or decimal operation.";
                invalidMetaDataCallback.throwOnInvalidMetaData("Redshift server returned invalid metadata for a numeric or decimal operation.");
                throw new IllegalArgumentException("invalidMetaDataCallbacks must throw an exception.");
            }
            return super.createTypeDescriptor(addColumnPropertyCallback, invalidMetaDataCallback, connection, table, metaData, columnLabel, colIndex);
        }
    }
}
