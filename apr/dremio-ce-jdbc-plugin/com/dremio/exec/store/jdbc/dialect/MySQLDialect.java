package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.jdbc.legacy.*;
import javax.sql.*;
import com.google.common.base.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import java.text.*;
import java.util.*;
import org.slf4j.*;
import java.sql.*;

public final class MySQLDialect extends ArpDialect
{
    private final ArpTypeMapper typeMapper;
    
    public MySQLDialect(final ArpYaml yaml) {
        super(yaml);
        this.typeMapper = new MySQLTypeMapper(yaml);
    }
    
    @Override
    public SqlNode emulateNullDirection(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return MysqlSqlDialect.DEFAULT.emulateNullDirection(node, nullsFirst, desc);
    }
    
    public SqlDialect.CalendarPolicy getCalendarPolicy() {
        return MysqlSqlDialect.DEFAULT.getCalendarPolicy();
    }
    
    @Override
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
            MysqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    @Override
    public boolean supportsLiteral(final CompleteType type) {
        return type.isTemporal() || super.supportsLiteral(type);
    }
    
    public SqlNode rewriteSingleValueExpr(final SqlNode aggCall) {
        return MysqlSqlDialect.DEFAULT.rewriteSingleValueExpr(aggCall);
    }
    
    public boolean useTimestampAddInsteadOfDatetimePlus() {
        return true;
    }
    
    @Override
    public MySQLRelToSqlConverter getConverter() {
        return new MySQLRelToSqlConverter(this);
    }
    
    @Override
    public MySQLSchemaFetcher getSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        final String query = String.format("SELECT * FROM (SELECT TABLE_SCHEMA CAT, NULL SCH, TABLE_NAME NME from information_schema.tables WHERE TABLE_TYPE NOT IN ('%s')) t WHERE UPPER(CAT) NOT IN ('%s')", Joiner.on("','").join((Iterable)config.getHiddenTableTypes()), Joiner.on("','").join((Iterable)config.getHiddenSchemas()));
        return new MySQLSchemaFetcher(query, name, dataSource, timeout, config);
    }
    
    public boolean supportsOver(final RexOver over) {
        return false;
    }
    
    public boolean supportsOver(final Window window) {
        return false;
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    public boolean coerceTimesToUTC() {
        return true;
    }
    
    @Override
    public TypeMapper getDataTypeMapper() {
        return this.typeMapper;
    }
    
    private static final class MySQLSchemaFetcher extends ArpSchemaFetcher
    {
        private static final Logger logger;
        
        private MySQLSchemaFetcher(final String query, final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
            super(query, name, dataSource, timeout, config);
        }
        
        @Override
        protected long getRowCount(final JdbcDatasetHandle handle) {
            final List<String> tablePath = handle.getIdentifiers();
            final String sql = MessageFormat.format("SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {0} AND TABLE_NAME = {1} AND ENGINE <> ''InnoDB''", this.config.getDialect().quoteStringLiteral((String)tablePath.get(0)), this.config.getDialect().quoteStringLiteral((String)tablePath.get(1)));
            final Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
            if (estimate.isPresent()) {
                return estimate.get();
            }
            MySQLSchemaFetcher.logger.debug("Row count estimate {} detected on table {}. Retrying with count_big query.", (Object)1000000000L, (Object)this.getQuotedPath(tablePath));
            return super.getRowCount(handle);
        }
        
        static {
            logger = LoggerFactory.getLogger((Class)MySQLSchemaFetcher.class);
        }
    }
    
    private static class MySQLTypeMapper extends ArpTypeMapper
    {
        public MySQLTypeMapper(final ArpYaml yaml) {
            super(yaml);
        }
        
        @Override
        protected SourceTypeDescriptor createTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final TableIdentifier table, final ResultSetMetaData metaData, final String columnLabel, final int colIndex) throws SQLException {
            final int colType = metaData.getColumnType(colIndex);
            int precision = metaData.getPrecision(colIndex);
            final int scale = metaData.getScale(colIndex);
            if (colType == 3 || colType == 2) {
                precision = Math.max(precision, 38);
            }
            return new SourceTypeDescriptor(columnLabel, colType, metaData.getColumnTypeName(colIndex), colIndex, precision, scale);
        }
    }
}
