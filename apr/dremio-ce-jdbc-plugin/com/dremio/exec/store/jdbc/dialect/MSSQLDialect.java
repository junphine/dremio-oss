package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.dremio.common.expression.*;
import org.apache.calcite.rel.type.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.store.jdbc.legacy.*;
import javax.sql.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.planner.sql.handlers.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import com.google.common.collect.*;
import com.google.common.base.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import org.slf4j.*;
import org.apache.calcite.sql.type.*;

public class MSSQLDialect extends ArpDialect
{
    public static final Set<SqlAggFunction> SUPPORTED_WINDOW_AGG_CALLS;
    private static final int MSSQL_MAX_VARCHAR_LENGTH = 8000;
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final boolean DISABLE_PUSH_COLLATION;
    private final SqlCollation MSSQL_BINARY_COLLATION;
    
    public MSSQLDialect(final ArpYaml yaml) {
        super(yaml);
        this.MSSQL_BINARY_COLLATION = new SqlCollation(SqlCollation.Coercibility.NONE) {
            private static final long serialVersionUID = 1L;
            
            public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
                writer.keyword("COLLATE");
                writer.keyword("Latin1_General_BIN2");
            }
        };
    }
    
    public void unparseDateTimeLiteral(final SqlWriter writer, final SqlAbstractDateTimeLiteral literal, final int leftPrec, final int rightPrec) {
        writer.literal("'" + literal.toFormattedString() + "'");
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    protected SqlNode emulateNullDirectionWithIsNull(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return this.emulateNullDirectionWithCaseIsNull(node, nullsFirst, desc);
    }
    
    @Override
    public boolean supportsSort(final Sort e) {
        boolean hasOrderBy = false;
        if (e.getCollationList() != null && !e.getCollationList().isEmpty()) {
            for (final RelCollation collation : e.getCollationList()) {
                if (!collation.getFieldCollations().isEmpty()) {
                    hasOrderBy = true;
                    break;
                }
            }
        }
        return (hasOrderBy || JdbcSort.isOffsetEmpty(e)) && super.supportsSort(e);
    }
    
    @Override
    public boolean supportsLiteral(final CompleteType type) {
        return !CompleteType.BIT.equals((Object)type) && super.supportsLiteral(type);
    }
    
    @Override
    public SqlNode getCastSpec(final RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR: {
                if (type.getPrecision() > 8000 || type.getPrecision() == -1) {
                    return getVarcharWithPrecision((DremioSqlDialect)this, type, 8000);
                }
                return getVarcharWithPrecision((DremioSqlDialect)this, type, type.getPrecision());
            }
            case TIMESTAMP: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("DATETIME2", SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
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
    
    public boolean useTimestampAddInsteadOfDatetimePlus() {
        return true;
    }
    
    @Override
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlOperator op = call.getOperator();
        if (op == SqlStdOperatorTable.TIMESTAMP_DIFF) {
            super.unparseCall(writer, MSSQLDialect.DATEDIFF.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (call instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect)call;
            if (select.getFetch() != null && (select.getOffset() == null || (long)((SqlLiteral)select.getOffset()).getValueAs((Class)Long.class) == 0L)) {
                final SqlNodeList keywords = new SqlNodeList(SqlParserPos.ZERO);
                if (select.getModifierNode(SqlSelectKeyword.DISTINCT) != null) {
                    keywords.add(select.getModifierNode(SqlSelectKeyword.DISTINCT));
                }
                else if (select.getModifierNode(SqlSelectKeyword.ALL) != null) {
                    keywords.add(select.getModifierNode(SqlSelectKeyword.ALL));
                }
                keywords.add((SqlNode)SqlSelectExtraKeyword.TOP.symbol(SqlParserPos.ZERO));
                keywords.add(select.getFetch());
                final SqlSelect modifiedSelect = SqlSelectOperator.INSTANCE.createCall(keywords, select.getSelectList(), select.getFrom(), select.getWhere(), select.getGroup(), select.getHaving(), select.getWindowList(), select.getOrderList(), (SqlNode)null, (SqlNode)null, SqlParserPos.ZERO);
                super.unparseCall(writer, (SqlCall)modifiedSelect, leftPrec, rightPrec);
            }
            else {
                super.unparseCall(writer, call, leftPrec, rightPrec);
            }
        }
        else if (op == SqlStdOperatorTable.SUBSTRING || (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) || op == SqlStdOperatorTable.TIMESTAMP_ADD) {
            MssqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    @Override
    public MSSQLRelToSqlConverter getConverter() {
        return new MSSQLRelToSqlConverter(this);
    }
    
    @Override
    public ArpSchemaFetcher getSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        return new MSSQLSchemaFetcher((Set)config.getHiddenSchemas(), name, dataSource, timeout, config);
    }
    
    public boolean requiresTrimOnChars() {
        return true;
    }
    
    public boolean supportsOver(final Window window) {
        for (final Window.Group group : window.groups) {
            final boolean notBounded = group.lowerBound == null && group.upperBound == null;
            for (final Window.RexWinAggCall aggCall : group.aggCalls) {
                final SqlAggFunction operator = (SqlAggFunction)aggCall.getOperator();
                final boolean hasEmptyFrame = notBounded || OverUtils.hasDefaultFrame(operator, group.isRows, group.lowerBound, group.upperBound, group.orderKeys.getFieldCollations().size());
                if (!hasEmptyFrame && !MSSQLDialect.SUPPORTED_WINDOW_AGG_CALLS.contains(operator)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public boolean supportsOver(final RexOver over) {
        final boolean hasEmptyFrame = (over.getWindow().getLowerBound() == null && over.getWindow().getUpperBound() == null) || OverUtils.hasDefaultFrame(over);
        return hasEmptyFrame || MSSQLDialect.SUPPORTED_WINDOW_AGG_CALLS.contains(over.getAggOperator());
    }
    
    public SqlCollation getDefaultCollation(final SqlKind kind) {
        if (MSSQLDialect.DISABLE_PUSH_COLLATION) {
            return null;
        }
        switch (kind) {
            case LITERAL:
            case IDENTIFIER: {
                return this.MSSQL_BINARY_COLLATION;
            }
            default: {
                return null;
            }
        }
    }
    
    static {
        SUPPORTED_WINDOW_AGG_CALLS = (Set)ImmutableSet.of((Object)SqlStdOperatorTable.COUNT, (Object)SqlStdOperatorTable.LAST_VALUE, (Object)SqlStdOperatorTable.FIRST_VALUE);
        DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.mssql.push-collation.disable");
    }
    
    private static final class MSSQLSchemaFetcher extends ArpSchemaFetcher
    {
        private static final Logger logger;
        
        private MSSQLSchemaFetcher(final Set<String> hiddenSchemas, final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
            super("SELECT * FROM @AllTables WHERE 1=1", name, dataSource, timeout, config, true, false);
        }
        
        @Override
        protected String filterQuery(final String query, final DatabaseMetaData metaData) throws SQLException {
            return "SET NOCOUNT ON;\nDECLARE @dbname nvarchar(128), @litdbname nvarchar(128)\nDECLARE @AllTables table (CAT sysname, SCH sysname, NME sysname)\nBEGIN\n    DECLARE DBCursor CURSOR FOR SELECT name FROM sys.sysdatabases WHERE HAS_DBACCESS(name) = 1\n    OPEN DBCursor\n    FETCH NEXT FROM DBCursor INTO @dbname\n    WHILE @@FETCH_STATUS = 0\n    BEGIN\n        IF @dbname <> 'tempdb' BEGIN\n            SET @litdbname = REPLACE(@dbname, '''', '''''')\n            INSERT INTO @AllTables (CAT, SCH, NME) \n                EXEC('SELECT ''' + @litdbname + ''' CAT, SCH, NME FROM (SELECT s.name SCH, t.name AS NME FROM [' + @dbname + '].sys.tables t with (nolock) INNER JOIN [' + @dbname + '].sys.schemas s with (nolock) ON t.schema_id=s.schema_id UNION ALL SELECT s.name SCH, v.name AS NME FROM [' + @dbname + '].sys.views v with (nolock) INNER JOIN [' + @dbname + '].sys.schemas s with (nolock) ON v.schema_id=s.schema_id) t  WHERE SCH NOT IN (''" + String.format("%s", Joiner.on("'',''").join((Iterable)this.config.getHiddenSchemas())) + "'')')\n        END\n        FETCH NEXT FROM DBCursor INTO @dbname\n    END\n    CLOSE DBCursor\n    DEALLOCATE DBCursor\n" + super.filterQuery(query, metaData) + "\nEND;\nSET NOCOUNT OFF;";
        }
        
        public long getRowCount(final JdbcDatasetHandle handle) {
            final List<String> tablePath = handle.getIdentifiers();
            final String sql = MessageFormat.format("SELECT p.rows \nFROM {0}.sys.tables AS tbl\nINNER JOIN {0}.sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2\nINNER JOIN {0}.sys.partitions AS p ON p.object_id=tbl.object_id\nAND p.index_id=idx.index_id\nWHERE ((tbl.name={2}\nAND SCHEMA_NAME(tbl.schema_id)={1}))", this.config.getDialect().quoteIdentifier((String)tablePath.get(0)), this.config.getDialect().quoteStringLiteral((String)tablePath.get(1)), this.config.getDialect().quoteStringLiteral((String)tablePath.get(2)));
            final String quotedPath = this.getQuotedPath(tablePath);
            final Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
            if (estimate.isPresent()) {
                return estimate.get();
            }
            MSSQLSchemaFetcher.logger.debug("Row count estimate {} detected on table {}. Retrying with count_big query.", (Object)1000000000L, (Object)quotedPath);
            final Optional<Long> fallbackEstimate = this.executeQueryAndGetFirstLong("SELECT COUNT_BIG(*) FROM " + quotedPath);
            return fallbackEstimate.orElse(1000000000L);
        }
        
        static {
            logger = LoggerFactory.getLogger((Class)MSSQLSchemaFetcher.class);
        }
    }
}
