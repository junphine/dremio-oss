package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.conf.*;
import java.util.function.*;
import java.util.stream.*;
import com.dremio.exec.store.jdbc.dialect.arp.transformer.*;
import com.dremio.exec.store.jdbc.*;
import org.apache.calcite.sql.fun.*;
import com.dremio.common.dialect.arp.transformer.*;
import org.apache.calcite.sql.*;
import java.util.*;
import com.dremio.exec.store.jdbc.dialect.*;
import com.google.common.base.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.avatica.util.*;
import org.apache.calcite.sql.type.*;
import com.google.common.collect.*;
import org.apache.calcite.rel.type.*;
import java.math.*;
import org.slf4j.*;

public final class JdbcExpressionSupportCheck implements RexVisitor<Boolean>
{
    private static final Logger logger;
    private final JdbcDremioSqlDialect dialect;
    private final RexBuilder builder;
    
    public static boolean hasOnlySupportedFunctions(final RexNode rex, final StoragePluginId pluginId, final RexBuilder builder) {
        final DialectConf<?, ?> conf = (DialectConf<?, ?>)pluginId.getConnectionConf();
        return (boolean)rex.accept((RexVisitor)new JdbcExpressionSupportCheck(conf.getDialect(), builder));
    }
    
    private JdbcExpressionSupportCheck(final JdbcDremioSqlDialect dialect, final RexBuilder builder) {
        this.dialect = dialect;
        this.builder = builder;
    }
    
    public Boolean visitInputRef(final RexInputRef paramRexInputRef) {
        return true;
    }
    
    public Boolean visitLocalRef(final RexLocalRef paramRexLocalRef) {
        return true;
    }
    
    public Boolean visitCall(final RexCall paramRexCall) {
        final List<RexNode> operands = (List<RexNode>)paramRexCall.getOperands();
        List<RelDataType> paramTypes = operands.stream().map((Function<? super Object, ?>)RexNode::getType).collect((Collector<? super Object, ?, List<RelDataType>>)Collectors.toList());
        JdbcExpressionSupportCheck.logger.debug("Evaluating support for {} with operand types {} and return type {}", new Object[] { paramRexCall.getOperator().getName(), paramTypes, paramRexCall.getType() });
        final CallTransformer transformer = this.dialect.getCallTransformer(paramRexCall);
        boolean supportsFunction;
        if (transformer == TimeUnitFunctionTransformer.INSTANCE) {
            JdbcExpressionSupportCheck.logger.debug("Operator {} has been identified as a time unit function. Checking support using supportsTimeUnitFunction().", (Object)paramRexCall.getOperator().getName());
            final TimeUnitRange timeUnitRange = EnumParameterUtils.getFirstParamAsTimeUnitRange(operands);
            supportsFunction = this.dialect.supportsTimeUnitFunction(paramRexCall.getOperator(), timeUnitRange, paramRexCall.getType(), (List)paramTypes);
        }
        else {
            SqlOperator operator;
            if (transformer != NoOpTransformer.INSTANCE) {
                operator = transformer.getAlternateOperator(paramRexCall);
                paramTypes = (List<RelDataType>)transformer.transformRexOperands((List)paramRexCall.operands).stream().map(RexNode::getType).collect(Collectors.toList());
            }
            else {
                operator = paramRexCall.getOperator();
            }
            JdbcExpressionSupportCheck.logger.debug("Verifying support for operator {} using supportsFunction().", (Object)paramRexCall.getOperator().getName());
            supportsFunction = (this.dialect.supportsFunction(operator, paramRexCall.getType(), (List)paramTypes) && (!"TO_DATE".equalsIgnoreCase(operator.getName()) || this.supportsDateTimeFormatString(operator, operands, 1)));
        }
        if (supportsFunction) {
            int i = 0;
            for (final RexNode operand : operands) {
                ++i;
                if (!(boolean)operand.accept((RexVisitor)this)) {
                    JdbcExpressionSupportCheck.logger.debug("Operand {} for operator {} was not supported. Aborting pushdown.", (Object)i, (Object)paramRexCall.getOperator().getName());
                    return false;
                }
            }
            JdbcExpressionSupportCheck.logger.debug("Operator {} was supported.", (Object)paramRexCall.getOperator().getName());
            return true;
        }
        if (this.dialect.useTimestampAddInsteadOfDatetimePlus() && (paramRexCall.getOperator() == SqlStdOperatorTable.DATETIME_PLUS || paramRexCall.getOperator() == SqlStdOperatorTable.DATETIME_MINUS)) {
            JdbcExpressionSupportCheck.logger.debug("Datetime + interval operation is unsupported, but dialect allows fallback to TIMESTAMPADD pushdown.");
            JdbcExpressionSupportCheck.logger.debug("Attempting to pushdown as TIMESTAMPADD.");
            return this.visitDatetimePlusAsTimestampAdd(paramRexCall);
        }
        JdbcExpressionSupportCheck.logger.debug("Operator {} was not supported. Aborting pushdown of a RelNode using this operator.", (Object)paramRexCall.getOperator().getName());
        return false;
    }
    
    public Boolean visitLiteral(final RexLiteral literal) {
        if (literal.getTypeName().isSpecial()) {
            return true;
        }
        JdbcExpressionSupportCheck.logger.debug("Literal of type {} encountered. Calling supportsLiteral().", (Object)literal.getType());
        return this.dialect.supportsLiteral(SourceTypeDescriptor.getType(literal.getType()));
    }
    
    public Boolean visitPatternFieldRef(final RexPatternFieldRef fieldRef) {
        return true;
    }
    
    public Boolean visitTableInputRef(final RexTableInputRef fieldRef) {
        return true;
    }
    
    public Boolean visitOver(final RexOver over) {
        if (!this.visitCall((RexCall)over)) {
            return false;
        }
        final RexWindow window = over.getWindow();
        for (final RexFieldCollation orderKey : window.orderKeys) {
            if (!(boolean)((RexNode)orderKey.left).accept((RexVisitor)this)) {
                return false;
            }
        }
        for (final RexNode partitionKey : window.partitionKeys) {
            if (!(boolean)partitionKey.accept((RexVisitor)this)) {
                return false;
            }
        }
        return true;
    }
    
    public Boolean visitCorrelVariable(final RexCorrelVariable paramRexCorrelVariable) {
        return true;
    }
    
    public Boolean visitDynamicParam(final RexDynamicParam paramRexDynamicParam) {
        return true;
    }
    
    public Boolean visitRangeRef(final RexRangeRef paramRexRangeRef) {
        return true;
    }
    
    public Boolean visitFieldAccess(final RexFieldAccess paramRexFieldAccess) {
        return (Boolean)paramRexFieldAccess.getReferenceExpr().accept((RexVisitor)this);
    }
    
    public Boolean visitSubQuery(final RexSubQuery subQuery) {
        for (final RexNode operand : subQuery.getOperands()) {
            if (!(boolean)operand.accept((RexVisitor)this)) {
                return false;
            }
        }
        return true;
    }
    
    private Boolean visitDatetimePlusAsTimestampAdd(final RexCall call) {
        Preconditions.checkState(this.dialect.useTimestampAddInsteadOfDatetimePlus());
        Preconditions.checkArgument(call.getOperator() == SqlStdOperatorTable.DATETIME_MINUS || call.getOperator() == SqlStdOperatorTable.DATETIME_PLUS);
        JdbcExpressionSupportCheck.logger.debug("Attempting to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD.");
        if (!RexUtil.isInterval((RexNode)call.getOperands().get(1))) {
            JdbcExpressionSupportCheck.logger.debug("The operand applied to DATETIME_PLUS was not an interval. Aborting pushdown.");
            return false;
        }
        if (!(boolean)call.getOperands().get(0).accept((RexVisitor)this)) {
            JdbcExpressionSupportCheck.logger.debug("The datetime expression applied to DATETIME_PLUS was not supported. Aborting pushdown.");
            return false;
        }
        final RexNode interval = call.getOperands().get(1);
        final RelDataTypeFactory factory = this.builder.getTypeFactory();
        final TimeUnit startUnit = interval.getType().getSqlTypeName().getStartUnit();
        final TimeUnit endUnit = interval.getType().getSqlTypeName().getEndUnit();
        TimeUnitRange timeUnit;
        if (startUnit == endUnit) {
            timeUnit = TimeUnitRange.of(startUnit, (TimeUnit)null);
        }
        else {
            timeUnit = TimeUnitRange.of(startUnit, endUnit);
        }
        Preconditions.checkNotNull((Object)timeUnit, (Object)"Time unit must be constructed correctly.");
        JdbcExpressionSupportCheck.logger.debug("Checking if TIMESTAMPADD is supported using supportsTimeUnitFunction()");
        if (!this.dialect.supportsTimeUnitFunction((SqlOperator)SqlStdOperatorTable.TIMESTAMP_ADD, timeUnit, call.getType(), (List)ImmutableList.of((Object)factory.createSqlType(SqlTypeName.ANY), (Object)factory.createSqlType(SqlTypeName.INTEGER), (Object)call.getOperands().get(0).getType()))) {
            JdbcExpressionSupportCheck.logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD .");
            return false;
        }
        JdbcExpressionSupportCheck.logger.debug("Checking if the interval operand in the DATETIME_PLUS operation is supported.");
        if (interval instanceof RexLiteral) {
            JdbcExpressionSupportCheck.logger.debug("Successfully pushed down conversion of DATETIME_PLUS to TIMESTAMPADD.");
            return true;
        }
        if (interval instanceof RexCall && ((RexCall)interval).getOperator() == SqlStdOperatorTable.MULTIPLY) {
            final RexCall multiplyOp = (RexCall)interval;
            final RexNode leftOp = multiplyOp.getOperands().get(0);
            final RexNode rightOp = multiplyOp.getOperands().get(1);
            if (RexUtil.isIntervalLiteral(leftOp)) {
                return this.canReplaceIntervalMultiplicationWithIntegerMultiplication(rightOp);
            }
            if (RexUtil.isIntervalLiteral(rightOp)) {
                return this.canReplaceIntervalMultiplicationWithIntegerMultiplication(leftOp);
            }
        }
        JdbcExpressionSupportCheck.logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD.");
        return false;
    }
    
    private boolean canReplaceIntervalMultiplicationWithIntegerMultiplication(final RexNode coefficient) {
        JdbcExpressionSupportCheck.logger.debug("Checking if Multiply is supported.");
        final RelDataType multiplyDataType = this.builder.deriveReturnType((SqlOperator)SqlStdOperatorTable.MULTIPLY, (List)ImmutableList.of((Object)coefficient, (Object)this.builder.makeExactLiteral(BigDecimal.ONE)));
        if (!this.dialect.supportsFunction((SqlOperator)SqlStdOperatorTable.MULTIPLY, multiplyDataType, (List)ImmutableList.of((Object)coefficient.getType(), (Object)this.builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))) {
            JdbcExpressionSupportCheck.logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD.");
            return false;
        }
        final boolean supported = (boolean)coefficient.accept((RexVisitor)this);
        if (supported) {
            JdbcExpressionSupportCheck.logger.debug("Successfully pushed down conversion of DATETIME_PLUS to TIMESTAMPADD.");
        }
        else {
            JdbcExpressionSupportCheck.logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD ");
        }
        return supported;
    }
    
    private boolean supportsDateTimeFormatString(final SqlOperator operator, final List<RexNode> operands, final int index) {
        if (operands.size() < index || !(operands.get(index) instanceof RexLiteral)) {
            JdbcExpressionSupportCheck.logger.debug("Operator {} was not supported due to a non-string literal value. Aborting pushdown.", (Object)operator.getName());
            return false;
        }
        final RexLiteral literal = (RexLiteral)operands.get(index);
        final String dateFormatStr = (String)literal.getValueAs((Class)String.class);
        if (null == dateFormatStr) {
            JdbcExpressionSupportCheck.logger.debug("Operator {} was not supported due to a non-string literal value. Aborting pushdown.", (Object)operator.getName());
            return false;
        }
        JdbcExpressionSupportCheck.logger.debug("Operator {} has been identified as having a datetime format string. Checking support using supportsDateTimeFormatString().", (Object)operator.getName());
        return this.dialect.supportsDateTimeFormatString(dateFormatStr);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcExpressionSupportCheck.class);
    }
}
