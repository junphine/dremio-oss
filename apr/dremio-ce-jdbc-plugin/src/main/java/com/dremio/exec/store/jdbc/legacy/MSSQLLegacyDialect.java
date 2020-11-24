package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.config.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.fun.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.sql.parser.*;
import java.util.*;
import org.apache.calcite.sql.dialect.*;
import com.dremio.exec.store.jdbc.dialect.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.expression.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.planner.sql.handlers.*;
import org.apache.calcite.rex.*;
import com.dremio.common.rel2sql.*;
import com.google.common.collect.*;
import org.apache.calcite.sql.type.*;

public final class MSSQLLegacyDialect extends LegacyDialect
{
    public static final MSSQLLegacyDialect INSTANCE;
    public static final Set<SqlAggFunction> SUPPORTED_WINDOW_AGG_CALLS;
    private static final int MSSQL_MAX_VARCHAR_LENGTH = 8000;
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final boolean DISABLE_PUSH_COLLATION;
    private final SqlCollation MSSQL_BINARY_COLLATION;
    
    private MSSQLLegacyDialect() {
        super(SqlDialect.DatabaseProduct.MSSQL, SqlDialect.DatabaseProduct.MSSQL.name(), "[", NullCollation.HIGH);
        this.MSSQL_BINARY_COLLATION = new SqlCollation(SqlCollation.Coercibility.NONE) {
            private static final long serialVersionUID = 1L;
            
            public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
                writer.keyword("COLLATE");
                writer.keyword("Chinese_PRC_CI_AS");
            }
        };
    }
    
    public boolean useTimestampAddInsteadOfDatetimePlus() {
        return true;
    }
    
    public void unparseOffsetFetch(final SqlWriter writer, final SqlNode offset, final SqlNode fetch) {
        writer.newlineAndIndent();
        final SqlWriter.Frame offsetFrame = writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
        writer.keyword("OFFSET");
        if (offset == null) {
            writer.literal("0");
        }
        else {
            offset.unparse(writer, -1, -1);
        }
        writer.keyword("ROWS");
        writer.endList(offsetFrame);
        if (fetch != null) {
            writer.newlineAndIndent();
            final SqlWriter.Frame fetchFrame = writer.startList(SqlWriter.FrameTypeEnum.FETCH);
            writer.keyword("FETCH");
            writer.keyword("NEXT");
            fetch.unparse(writer, -1, -1);
            writer.keyword("ROWS");
            writer.keyword("ONLY");
            writer.endList(fetchFrame);
        }
    }
    
    public void unparseDateTimeLiteral(final SqlWriter writer, final SqlAbstractDateTimeLiteral literal, final int leftPrec, final int rightPrec) {
        writer.literal("'" + literal.toFormattedString() + "'");
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    public boolean supportsFunction(final SqlOperator operator, final RelDataType type, final List<RelDataType> paramTypes) {
        return operator != SqlStdOperatorTable.DATETIME_MINUS && operator != SqlStdOperatorTable.DATETIME_PLUS && super.supportsFunction(operator, type, (List)paramTypes);
    }
    
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
    
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlOperator op = call.getOperator();
        if (op == SqlStdOperatorTable.TRUNCATE) {
            final List<SqlNode> modifiedOperands = Lists.newArrayList();
            modifiedOperands.addAll(call.getOperandList());
            modifiedOperands.add((SqlNode)SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
            super.unparseCall(writer, SqlStdOperatorTable.ROUND.createCall(new SqlNodeList((Collection)modifiedOperands, SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.TIMESTAMP_DIFF) {
            super.unparseCall(writer, MSSQLLegacyDialect.DATEDIFF.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.SUBSTRING && call.operandCount() == 2) {
            final List<SqlNode> modifiedOperands = Lists.newArrayList();
            modifiedOperands.addAll(call.getOperandList());
            modifiedOperands.add((SqlNode)SqlLiteral.createExactNumeric(String.valueOf(Long.MAX_VALUE), SqlParserPos.ZERO));
            MssqlSqlDialect.DEFAULT.unparseCall(writer, SqlStdOperatorTable.SUBSTRING.createCall(new SqlNodeList((Collection)modifiedOperands, SqlParserPos.ZERO)), leftPrec, rightPrec);
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
    public JdbcDremioRelToSqlConverter getConverter() {
        return new MSSQLLegacyRelToSqlConverter(this);
    }
    
    public boolean requiresTrimOnChars() {
        return true;
    }
    
    public boolean supportsLiteral(final CompleteType type) {
        return !CompleteType.BIT.equals(type) && super.supportsLiteral(type);
    }
    
    public boolean supportsBooleanAggregation() {
        return false;
    }
    
    public boolean supportsSort(final boolean isCollationEmpty, final boolean isOffsetEmpty) {
        return !isCollationEmpty || isOffsetEmpty;
    }
    
    public boolean supportsOver(final Window window) {
        for (final Window.Group group : window.groups) {
            final boolean notBounded = group.lowerBound == null && group.upperBound == null;
            for (final Window.RexWinAggCall aggCall : group.aggCalls) {
                final SqlAggFunction operator = (SqlAggFunction)aggCall.getOperator();
                final boolean hasEmptyFrame = notBounded || OverUtils.hasDefaultFrame(operator, group.isRows, group.lowerBound, group.upperBound, group.orderKeys.getFieldCollations().size());
                if (!hasEmptyFrame && !MSSQLLegacyDialect.SUPPORTED_WINDOW_AGG_CALLS.contains(operator)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public boolean supportsOver(final RexOver over) {
        final boolean hasEmptyFrame = (over.getWindow().getLowerBound() == null && over.getWindow().getUpperBound() == null) || OverUtils.hasDefaultFrame(over);
        return hasEmptyFrame || MSSQLLegacyDialect.SUPPORTED_WINDOW_AGG_CALLS.contains(over.getAggOperator());
    }
    
    public SqlCollation getDefaultCollation(final SqlKind kind) {
        if (MSSQLLegacyDialect.DISABLE_PUSH_COLLATION) {
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
        INSTANCE = new MSSQLLegacyDialect();
        SUPPORTED_WINDOW_AGG_CALLS = ImmutableSet.of(SqlStdOperatorTable.COUNT, SqlStdOperatorTable.LAST_VALUE, SqlStdOperatorTable.FIRST_VALUE);
        DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.mssql.push-collation.disable");
    }
}
