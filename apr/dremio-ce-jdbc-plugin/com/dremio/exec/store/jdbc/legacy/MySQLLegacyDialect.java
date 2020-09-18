package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.config.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.parser.*;
import java.util.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.core.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.sql.type.*;

public final class MySQLLegacyDialect extends LegacyDialect
{
    public static final MySQLLegacyDialect INSTANCE;
    private static final int MYSQL_DECIMAL_MAX_PRECISION = 65;
    private static final int MYSQL_DECIMAL_DEFAULT_SCALE = 20;
    
    private MySQLLegacyDialect() {
        super(SqlDialect.DatabaseProduct.MYSQL, SqlDialect.DatabaseProduct.MYSQL.name(), "`", NullCollation.HIGH);
    }
    
    public void unparseOffsetFetch(final SqlWriter writer, final SqlNode offset, final SqlNode fetch) {
        MysqlSqlDialect.DEFAULT.unparseOffsetFetch(writer, offset, fetch);
    }
    
    public SqlNode emulateNullDirection(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return MysqlSqlDialect.DEFAULT.emulateNullDirection(node, nullsFirst, desc);
    }
    
    public boolean supportsAggregateFunction(final SqlKind kind) {
        return MysqlSqlDialect.DEFAULT.supportsAggregateFunction(kind);
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    public SqlDialect.CalendarPolicy getCalendarPolicy() {
        return MysqlSqlDialect.DEFAULT.getCalendarPolicy();
    }
    
    public SqlNode getCastSpec(final RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("CHAR", SqlParserPos.ZERO), type.getPrecision(), -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            case INTEGER: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("_UNSIGNED", SqlParserPos.ZERO), type.getPrecision(), -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            case BIGINT: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("_SIGNED INTEGER", SqlParserPos.ZERO), type.getPrecision(), -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            case TIMESTAMP: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("_DATETIME", SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            case DOUBLE: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier("DECIMAL", SqlParserPos.ZERO), 65, 20, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            default: {
                return super.getCastSpec(type);
            }
        }
    }
    
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
            MysqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    public SqlNode rewriteSingleValueExpr(final SqlNode aggCall) {
        return MysqlSqlDialect.DEFAULT.rewriteSingleValueExpr(aggCall);
    }
    
    @Override
    public JdbcDremioRelToSqlConverter getConverter() {
        return new MySQLLegacyRelToSqlConverter(this);
    }
    
    public boolean requiresTrimOnChars() {
        return true;
    }
    
    public boolean supportsSort(final boolean isCollationEmpty, final boolean isOffsetEmpty) {
        return true;
    }
    
    public boolean supportsOver(final RexOver over) {
        return false;
    }
    
    public boolean supportsOver(final Window window) {
        return false;
    }
    
    static {
        INSTANCE = new MySQLLegacyDialect();
    }
}
