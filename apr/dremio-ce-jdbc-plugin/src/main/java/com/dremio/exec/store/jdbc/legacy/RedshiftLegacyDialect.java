package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.config.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.dialect.*;
import java.util.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.core.*;
import com.google.common.collect.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;

public final class RedshiftLegacyDialect extends LegacyDialect
{
    public static final RedshiftLegacyDialect INSTANCE;
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final SqlFunction LOG;
    private static final SqlFunction TRUNC;
    
    private RedshiftLegacyDialect() {
        super(SqlDialect.DatabaseProduct.REDSHIFT, SqlDialect.DatabaseProduct.REDSHIFT.name(), "\"", NullCollation.HIGH);
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    public boolean supportsFunction(final SqlOperator operator, final RelDataType type, final List<RelDataType> paramTypes) {
        return operator != SqlStdOperatorTable.DATETIME_MINUS && operator != SqlStdOperatorTable.DATETIME_PLUS && super.supportsFunction(operator, type, (List)paramTypes);
    }
    
    public boolean useTimestampAddInsteadOfDatetimePlus() {
        return true;
    }
    
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlOperator op = call.getOperator();
        if (op == SqlStdOperatorTable.LOG10) {
            super.unparseCall(writer, RedshiftLegacyDialect.LOG.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.TRUNCATE) {
            super.unparseCall(writer, RedshiftLegacyDialect.TRUNC.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.TIMESTAMP_DIFF) {
            super.unparseCall(writer, RedshiftLegacyDialect.DATEDIFF.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.TIMESTAMP_ADD) {
            MssqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
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
    public JdbcDremioRelToSqlConverter getConverter() {
        return new JdbcDremioRelToSqlConverterBase(this);
    }
    
    public boolean supportsBooleanAggregation() {
        return false;
    }
    
    public boolean supportsFetchOffsetInSetOperand() {
        return false;
    }
    
    public boolean removeDefaultWindowFrame(final RexOver over) {
        return SqlKind.AGGREGATE.contains(over.getAggOperator().getKind());
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
    
    public boolean supportsOver(final RexOver over) {
        return over.getWindow() != null && over.getWindow().isRows();
    }
    
    static {
        INSTANCE = new RedshiftLegacyDialect();
        LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);
        TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
    }
}
