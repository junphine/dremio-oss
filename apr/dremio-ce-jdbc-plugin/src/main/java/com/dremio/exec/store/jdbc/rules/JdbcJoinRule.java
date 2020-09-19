package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.common.expression.*;
import org.apache.calcite.sql.fun.*;
import java.math.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.util.*;
import com.google.common.annotations.*;
import com.dremio.exec.store.jdbc.dialect.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import java.util.function.*;
import java.util.stream.*;
import java.util.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.*;
import org.slf4j.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.sql.type.*;

public final class JdbcJoinRule extends JdbcBinaryConverterRule
{
    private static final Logger logger;
    public static final JdbcJoinRule CALCITE_INSTANCE;
    public static final JdbcJoinRule LOGICAL_INSTANCE;
    
    private JdbcJoinRule(final Class<? extends Join> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public boolean matchImpl(final RelOptRuleCall call) {
        final Join join = (Join)call.rel(0);
        final JdbcCrel leftChild = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = leftChild.getPluginId();
        if (pluginId == null) {
            return true;
        }
        final JdbcDremioSqlDialect dialect = JdbcConverterRule.getDialect(pluginId);
        final JoinRelType joinType = join.getJoinType();
        boolean isJoinTypeSupported = false;
        if (joinType == JoinRelType.INNER && (join.getCondition() == null || join.getCondition().isAlwaysTrue())) {
            JdbcJoinRule.logger.debug("Checking if CROSS JOIN is supported by dialect.");
            isJoinTypeSupported = dialect.supportsJoin(JoinType.CROSS);
        }
        else {
            switch (joinType) {
                case INNER: {
                    isJoinTypeSupported = dialect.supportsJoin(JoinType.INNER);
                    break;
                }
                case LEFT: {
                    isJoinTypeSupported = dialect.supportsJoin(JoinType.LEFT);
                    break;
                }
                case RIGHT: {
                    isJoinTypeSupported = dialect.supportsJoin(JoinType.RIGHT);
                    break;
                }
                default: {
                    isJoinTypeSupported = dialect.supportsJoin(JoinType.FULL);
                    break;
                }
            }
        }
        if (isJoinTypeSupported) {
            JdbcJoinRule.logger.debug("Join type is supported.");
            return true;
        }
        JdbcJoinRule.logger.debug("Join type is not supported.");
        return false;
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel left, final JdbcCrel right, final StoragePluginId pluginId) {
        final Join join = (Join)rel;
        final RexNode originalCondition = join.getCondition();
        if (!satisfiesPrecondition(originalCondition)) {
            assert false : String.format("'%s' is not supported", originalCondition);
            return null;
        }
        else {
            final JdbcDremioSqlDialect dialect = (null == pluginId) ? null : JdbcConverterRule.getDialect(pluginId);
            final Pair<Boolean, Boolean> canJoin = canJoinOnCondition(originalCondition, dialect);
            if (!(boolean)canJoin.left) {
                return tryFilterOnJoin(join, left, right, pluginId);
            }
            RexNode newCondition;
            if ((boolean)canJoin.right && null != pluginId && !dialect.supportsLiteral(CompleteType.BIT)) {
                final RexBuilder builder = rel.getCluster().getRexBuilder();
                final RexNode falseCall = builder.makeCall((SqlOperator)SqlStdOperatorTable.GREATER_THAN, new RexNode[] { builder.makeBigintLiteral(BigDecimal.ZERO), builder.makeBigintLiteral(BigDecimal.ONE) });
                final RexNode trueCall = builder.makeCall((SqlOperator)SqlStdOperatorTable.GREATER_THAN, new RexNode[] { builder.makeBigintLiteral(BigDecimal.ONE), builder.makeBigintLiteral(BigDecimal.ZERO) });
                final RexShuttle shuttle = new RexShuttle() {
                    public RexNode visitLiteral(final RexLiteral literal) {
                        if (literal.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                            return (RexNode)literal;
                        }
                        final boolean value = RexLiteral.booleanValue((RexNode)literal);
                        if (value) {
                            return trueCall;
                        }
                        return falseCall;
                    }
                };
                newCondition = shuttle.apply(originalCondition);
            }
            else {
                newCondition = originalCondition;
            }
            return (RelNode)new JdbcJoin(join.getCluster(), join.getTraitSet().replace((RelTrait)Rel.LOGICAL), left.getInput(), right.getInput(), newCondition, join.getVariablesSet(), join.getJoinType(), pluginId);
        }
    }
    
    @VisibleForTesting
    static boolean satisfiesPrecondition(final RexNode rexNode) {
        return rexNode.isAlwaysTrue() || rexNode.isAlwaysFalse() || rexNode instanceof RexCall;
    }
    
    @VisibleForTesting
    static Pair<Boolean, Boolean> canJoinOnCondition(final RexNode node, final JdbcDremioSqlDialect dialect) {
        switch (node.getKind()) {
            case LITERAL: {
                final RexLiteral literal = (RexLiteral)node;
                Label_0252: {
                    switch (literal.getTypeName().getFamily()) {
                        case BOOLEAN: {
                            return (Pair<Boolean, Boolean>)Pair.of(true, true);
                        }
                        case CHARACTER:
                        case NUMERIC:
                        case EXACT_NUMERIC:
                        case APPROXIMATE_NUMERIC:
                        case INTERVAL_YEAR_MONTH:
                        case INTERVAL_DAY_TIME:
                        case DATE:
                        case TIME:
                        case TIMESTAMP: {
                            if (null != dialect && !dialect.supportsLiteral(SourceTypeDescriptor.getType(literal.getType()))) {
                                return (Pair<Boolean, Boolean>)Pair.of(false, false);
                            }
                            return (Pair<Boolean, Boolean>)Pair.of(true, false);
                        }
                        case ANY:
                        case NULL: {
                            switch (literal.getTypeName()) {
                                case NULL: {
                                    return (Pair<Boolean, Boolean>)Pair.of(true, false);
                                }
                                default: {
                                    break Label_0252;
                                }
                            }
                            //break;
                        }
                    }
                }
                return (Pair<Boolean, Boolean>)Pair.of(false, false);
            }
            case AND:
            case OR: {
                assert node instanceof RexCall;
                boolean foundBoolean = false;
                for (final RexNode operand : ((RexCall)node).getOperands()) {
                    if (!satisfiesPrecondition(operand) && !operand.getKind().equals(SqlKind.INPUT_REF) && !operand.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN) && !dialect.supportsLiteral(CompleteType.BIT)) {
                        return (Pair<Boolean, Boolean>)Pair.of(false, foundBoolean);
                    }
                    final Pair<Boolean, Boolean> opResult = canJoinOnCondition(operand, dialect);
                    if (!(boolean)opResult.left) {
                        return (Pair<Boolean, Boolean>)Pair.of(false, opResult.right);
                    }
                    if (!(boolean)opResult.right) {
                        continue;
                    }
                    foundBoolean = true;
                }
                return (Pair<Boolean, Boolean>)Pair.of(true, foundBoolean);
            }
            case IS_NOT_DISTINCT_FROM:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case EQUALS: {
                assert node instanceof RexCall;
                boolean foundBoolean = false;
                for (final RexNode operand : ((RexCall)node).getOperands()) {
                    final Pair<Boolean, Boolean> opResult = canJoinOnCondition(operand, dialect);
                    if (!(boolean)opResult.left) {
                        return (Pair<Boolean, Boolean>)Pair.of(false, opResult.right);
                    }
                    if (!(boolean)opResult.right) {
                        continue;
                    }
                    foundBoolean = true;
                }
                return (Pair<Boolean, Boolean>)Pair.of(true, foundBoolean);
            }
            case IS_NULL:
            case IS_NOT_NULL: {
                final List<RexNode> operands = (List<RexNode>)((RexCall)node).getOperands();
                return (Pair<Boolean, Boolean>)Pair.of((operands.size() == 1 && operands.get(0) instanceof RexInputRef), false);
            }
            case INPUT_REF: {
                return (Pair<Boolean, Boolean>)Pair.of(true, false);
            }
            default: {
                if (null != dialect && node instanceof RexCall) {
                    final RexCall call = (RexCall)node;
                    if (call.getOperator() instanceof SqlFunction && ((SqlFunction)call.getOperator()).getFunctionType().isFunction()) {
                        if (!dialect.supportsFunction(call.getOperator(), call.getType(), (List)call.getOperands().stream().map(RexNode::getType).collect(Collectors.toList()))) {
                            return (Pair<Boolean, Boolean>)Pair.of(false, false);
                        }
                        boolean foundBoolean2 = false;
                        for (final RexNode operand2 : ((RexCall)node).getOperands()) {
                            final Pair<Boolean, Boolean> opResult2 = canJoinOnCondition(operand2, dialect);
                            if (!(boolean)opResult2.left) {
                                return (Pair<Boolean, Boolean>)Pair.of(false, opResult2.right);
                            }
                            if (!(boolean)opResult2.right) {
                                continue;
                            }
                            foundBoolean2 = true;
                        }
                        return (Pair<Boolean, Boolean>)Pair.of(true, foundBoolean2);
                    }
                }
                return (Pair<Boolean, Boolean>)Pair.of(false, false);
            }
        }
    }
    
    private static RelNode tryFilterOnJoin(final Join join, final JdbcCrel left, final JdbcCrel right, final StoragePluginId pluginId) {
        if (join.getJoinType() != JoinRelType.INNER) {
            return null;
        }
        final RelOptCluster cluster = join.getCluster();
        final RexBuilder builder = cluster.getRexBuilder();
        final RexNode trueCondition = builder.makeCall((SqlOperator)SqlStdOperatorTable.GREATER_THAN, new RexNode[] { builder.makeBigintLiteral(BigDecimal.ONE), builder.makeBigintLiteral(BigDecimal.ZERO) });
        final JdbcJoin newJoin = new JdbcJoin(cluster, join.getTraitSet().replace((RelTrait)Rel.LOGICAL), left.getInput(), right.getInput(), trueCondition, join.getVariablesSet(), join.getJoinType(), pluginId);
        final JdbcCrel fauxCrel = new JdbcCrel(cluster, newJoin.getTraitSet().replace((RelTrait)Rel.LOGICAL), (RelNode)newJoin, pluginId);
        final LogicalFilter logicalFilter = LogicalFilter.create((RelNode)fauxCrel, join.getCondition());
        if (!JdbcFilterRule.matches((Filter)logicalFilter, fauxCrel)) {
            return null;
        }
        return JdbcFilterRule.convert((Filter)logicalFilter, fauxCrel, pluginId);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcJoinRule.class);
        CALCITE_INSTANCE = new JdbcJoinRule((Class<? extends Join>)LogicalJoin.class, "JdbcJoinRuleCrel");
        LOGICAL_INSTANCE = new JdbcJoinRule((Class<? extends Join>)JoinRel.class, "JdbcJoinRuleDrel");
    }
}
