package com.dremio.exec.store.jdbc.dialect.arp.transformer;

import com.dremio.common.dialect.arp.transformer.*;
import com.google.common.collect.*;
import java.util.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.*;
import java.util.function.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.sql.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.parser.*;

public final class TrimTransformer extends CallTransformer
{
    public static final TrimTransformer INSTANCE;
    private static final ImmutableSet<SqlOperator> operators;
    
    public boolean matches(final RexCall call) {
        if (!this.matches(call.op)) {
            return false;
        }
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        if (operands.size() != 3) {
            return false;
        }
        final RexNode charArg = operands.get(1);
        if (charArg.getKind() != SqlKind.LITERAL) {
            return false;
        }
        final String literalValue = (String)((RexLiteral)charArg).getValueAs((Class)String.class);
        return " ".equals(literalValue);
    }
    
    public Set<SqlOperator> getCompatibleOperators() {
        return (Set<SqlOperator>)TrimTransformer.operators;
    }
    
    public List<SqlNode> transformSqlOperands(final List<SqlNode> operands) {
        return operands.subList(2, 3);
    }
    
    public List<RexNode> transformRexOperands(final List<RexNode> operands) {
        return operands.subList(2, 3);
    }
    
    public String adjustNameBasedOnOperands(final String operatorName, final List<RexNode> operands) {
        final RexNode firstOp = operands.get(0);
        final RexLiteral asLiteral = (RexLiteral)firstOp;
        final SqlTrimFunction.Flag value = (SqlTrimFunction.Flag)asLiteral.getValueAs((Class)SqlTrimFunction.Flag.class);
        switch (value) {
            case LEADING: {
                return "LTRIM";
            }
            case TRAILING: {
                return "RTRIM";
            }
            default: {
                return "TRIM";
            }
        }
    }
    
    public SqlOperator getAlternateOperator(final RexCall call) {
        final RexNode firstOp = (RexNode)call.operands.get(0);
        final RexLiteral asLiteral = (RexLiteral)firstOp;
        final SqlTrimFunction.Flag value = (SqlTrimFunction.Flag)asLiteral.getValueAs((Class)SqlTrimFunction.Flag.class);
        switch (value) {
            case LEADING: {
                return (SqlOperator)OracleSqlOperatorTable.LTRIM;
            }
            case TRAILING: {
                return (SqlOperator)OracleSqlOperatorTable.RTRIM;
            }
            default: {
                return (SqlOperator)SqlStdOperatorTable.TRIM;
            }
        }
    }
    
    public Supplier<SqlNode> getAlternateCall(final Supplier<SqlNode> originalNodeSupplier, final DremioRelToSqlConverter.DremioContext context, final RexProgram program, final RexCall call) {
        final String trimFuncName;
        final List<SqlNode> operands;
        final SqlOperatorImpl function;
        return (Supplier<SqlNode>)(() -> {
            trimFuncName = this.adjustNameBasedOnOperands(call.getOperator().getName(), call.getOperands());
            operands = (List<SqlNode>)context.toSql(program, (List)this.transformRexOperands(call.getOperands()));
            function = new SqlOperatorImpl(trimFuncName, operands.size(), operands.size(), true, (SqlReturnTypeInference)DynamicReturnType.INSTANCE);
            return function.createCall(SqlParserPos.ZERO, (List)operands);
        });
    }
    
    static {
        INSTANCE = new TrimTransformer();
        final ImmutableSet.Builder<SqlOperator> opBuilder = (ImmutableSet.Builder<SqlOperator>)ImmutableSet.builder();
        operators = opBuilder.add((Object)SqlStdOperatorTable.TRIM).build();
    }
}
