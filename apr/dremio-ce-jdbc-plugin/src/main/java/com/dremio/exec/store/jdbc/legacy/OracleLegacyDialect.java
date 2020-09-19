package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.config.*;
import org.apache.calcite.rel.type.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.fun.*;
import com.google.common.collect.*;
import java.util.*;
import org.apache.calcite.sql.dialect.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.expression.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.sql.*;
import com.google.common.base.*;
import org.apache.calcite.sql.type.*;

public final class OracleLegacyDialect extends LegacyDialect
{
    public static final OracleLegacyDialect INSTANCE;
    private static final int ORACLE_MAX_VARCHAR_LENGTH = 4000;
    private static final SqlFunction LOG;
    private static final SqlFunction TRUNC;
    
    private OracleLegacyDialect() {
        super(SqlDialect.DatabaseProduct.ORACLE, SqlDialect.DatabaseProduct.ORACLE.name(), "\"", NullCollation.HIGH);
    }
    
    public SqlNode getCastSpec(final RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR: {
                if (type.getPrecision() > 4000 || type.getPrecision() == -1) {
                    return getVarcharWithPrecision((DremioSqlDialect)this, type, 4000);
                }
                return getVarcharWithPrecision((DremioSqlDialect)this, type, type.getPrecision());
            }
            case DOUBLE: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier(OracleKeyWords.NUMBER.toString(), SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO) {
                    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
                        writer.keyword(OracleKeyWords.NUMBER.toString());
                    }
                };
            }
            default: {
                return super.getCastSpec(type);
            }
        }
    }
    
    public boolean supportsAliasedValues() {
        return false;
    }
    
    public boolean supportsFunction(final SqlOperator operator, final RelDataType type, final List<RelDataType> paramTypes) {
        return operator != SqlStdOperatorTable.TIMESTAMP_DIFF && super.supportsFunction(operator, type, (List)paramTypes);
    }
    
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlOperator op = call.getOperator();
        if (op == SqlStdOperatorTable.LOG10) {
            final List<SqlNode> modifiedOperands = Lists.newArrayList();
            modifiedOperands.add((SqlNode)SqlLiteral.createExactNumeric("10.0", SqlParserPos.ZERO));
            modifiedOperands.addAll(call.getOperandList());
            super.unparseCall(writer, OracleLegacyDialect.LOG.createCall(new SqlNodeList((Collection)modifiedOperands, SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else if ((call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) || op == SqlStdOperatorTable.SUBSTRING) {
            OracleSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else if (op == SqlStdOperatorTable.TRUNCATE) {
            super.unparseCall(writer, OracleLegacyDialect.TRUNC.createCall(new SqlNodeList((Collection)call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    protected boolean allowsAs() {
        return false;
    }
    
    @Override
    public JdbcDremioRelToSqlConverter getConverter() {
        return new OracleLegacyRelToSqlConverter(this);
    }
    
    public boolean supportsLiteral(final CompleteType type) {
        return !CompleteType.BIT.equals(type);
    }
    
    public boolean supportsBooleanAggregation() {
        return false;
    }
    
    public boolean supportsSort(final boolean isCollationEmpty, final boolean isOffsetEmpty) {
        return isOffsetEmpty;
    }
    
    static {
        INSTANCE = new OracleLegacyDialect();
        LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)OperandTypes.NUMERIC_NUMERIC, SqlFunctionCategory.NUMERIC);
        TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
    }
    
    public enum OracleKeyWords
    {
        NUMBER("NUMBER"), 
        ROWNUM("ROWNUM");
        
        private final String name;
        
        private OracleKeyWords(final String name) {
            this.name = (String)Preconditions.checkNotNull(name);
        }
        
        @Override
        public String toString() {
            return this.name;
        }
    }
}
