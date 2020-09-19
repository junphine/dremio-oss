package com.dremio.exec.store.jdbc.dialect.arp.transformer;

import com.dremio.common.dialect.arp.transformer.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.store.jdbc.*;
import org.apache.calcite.avatica.util.*;
import java.util.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.planner.sql.*;
import org.apache.calcite.sql.fun.*;

public final class TimeUnitFunctionTransformer extends CallTransformer
{
    public static final TimeUnitFunctionTransformer INSTANCE;
    private static final ImmutableSet<SqlOperator> operators;
    
    public boolean matches(final RexCall call) {
        if (!this.matches(call.op)) {
            return false;
        }
        if (call.operands.isEmpty()) {
            return false;
        }
        final RexNode firstOperand = (RexNode)call.operands.get(0);
        if (firstOperand.getKind() != SqlKind.LITERAL) {
            return false;
        }
        final RexLiteral firstAsLiteral = (RexLiteral)firstOperand;
        if (call.op.getName().equalsIgnoreCase("DATE_TRUNC")) {
            return firstAsLiteral.getValue2() instanceof String && EnumParameterUtils.TIME_UNIT_MAPPING.containsKey(((String)firstAsLiteral.getValueAs((Class)String.class)).toLowerCase(Locale.ROOT));
        }
        return firstAsLiteral.getTypeName().isSpecial() && (firstAsLiteral.getValue() instanceof TimeUnit || firstAsLiteral.getValue() instanceof TimeUnitRange);
    }
    
    public Set<SqlOperator> getCompatibleOperators() {
        return (Set<SqlOperator>)TimeUnitFunctionTransformer.operators;
    }
    
    public List<SqlNode> transformSqlOperands(final List<SqlNode> operands) {
        return operands.subList(1, operands.size());
    }
    
    public List<RexNode> transformRexOperands(final List<RexNode> operands) {
        return operands.subList(1, operands.size());
    }
    
    public String adjustNameBasedOnOperands(final String operatorName, final List<RexNode> operands) {
        final TimeUnitRange range = EnumParameterUtils.getFirstParamAsTimeUnitRange(operands);
        return operatorName + "_" + range.toString();
    }
    
    static {
        INSTANCE = new TimeUnitFunctionTransformer();
        final ImmutableSet.Builder<SqlOperator> opBuilder = ImmutableSet.builder();
        operators = opBuilder.add(new SqlOperatorImpl("DATE_TRUNC", 2, true)).add(SqlStdOperatorTable.TIMESTAMP_ADD).add(SqlStdOperatorTable.TIMESTAMP_DIFF).add(SqlStdOperatorTable.EXTRACT).build();
    }
}
