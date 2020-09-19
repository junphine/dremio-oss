package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.config.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.dialect.*;
import java.util.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;

public final class PostgreSQLLegacyDialect extends LegacyDialect
{
    public static final PostgreSQLLegacyDialect INSTANCE;
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final SqlFunction LOG;
    private static final SqlFunction TRUNC;
    
    private PostgreSQLLegacyDialect() {
        super(SqlDialect.DatabaseProduct.POSTGRESQL, SqlDialect.DatabaseProduct.POSTGRESQL.name(), "\"", NullCollation.HIGH);
    }
    
    protected boolean requiresAliasForFromItems() {
        return true;
    }
    
    public boolean supportsNestedAggregations() {
        return false;
    }
    
    public boolean supportsFunction(final SqlOperator operator, final RelDataType type, final List<RelDataType> paramTypes) {
        return operator != SqlStdOperatorTable.TIMESTAMP_DIFF && super.supportsFunction(operator, type, (List)paramTypes);
    }
    
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlOperator op = call.getOperator();
        if (op == SqlStdOperatorTable.LOG10) {
            super.unparseCall(writer, PostgreSQLLegacyDialect.LOG.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.TRUNCATE) {
            super.unparseCall(writer, PostgreSQLLegacyDialect.TRUNC.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
            PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
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
    
    public boolean supportsSort(final boolean isCollationEmpty, final boolean isOffsetEmpty) {
        return true;
    }
    
    public boolean supportsFetchOffsetInSetOperand() {
        return false;
    }
    
    static {
        INSTANCE = new PostgreSQLLegacyDialect();
        LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);
        TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
    }
}
