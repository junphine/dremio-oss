package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.util.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;
import com.dremio.exec.store.jdbc.legacy.*;
import org.apache.calcite.rel.type.*;
import com.dremio.exec.store.jdbc.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import java.util.*;
import org.slf4j.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;

public class JdbcAggregateRule extends JdbcUnaryConverterRule
{
    private static final Logger logger;
    public static final JdbcAggregateRule CALCITE_INSTANCE;
    public static final JdbcAggregateRule LOGICAL_INSTANCE;
    
    private JdbcAggregateRule(final Class<? extends Aggregate> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final Aggregate agg = (Aggregate)rel;
        if (agg.getGroupSets().size() != 1) {
            return null;
        }
        try {
            return (RelNode)new JdbcAggregate(rel.getCluster(), crel.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), agg.indicator, agg.getGroupSet(), (List<ImmutableBitSet>)agg.getGroupSets(), agg.getAggCallList(), pluginId);
        }
        catch (InvalidRelException e) {
            JdbcAggregateRule.logger.debug(e.toString());
            return null;
        }
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final Aggregate aggregate = (Aggregate)call.rel(0);
        final JdbcCrel crel = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = crel.getPluginId();
        if (pluginId == null) {
            return true;
        }
        final JdbcDremioSqlDialect dialect = JdbcConverterRule.getDialect(pluginId);
        JdbcAggregateRule.logger.debug("Checking if source RDBMS supports aggregation.");
        if (!dialect.supportsAggregation()) {
            JdbcAggregateRule.logger.debug("Aggregations are unsupported.");
            return false;
        }
        if (this.hasUnpushableTypes(aggregate)) {
            return false;
        }
        for (final AggregateCall aggCall : aggregate.getAggCallList()) {
            JdbcAggregateRule.logger.debug("Aggregate expression: {}", (Object)aggCall);
            if (!dialect.supportsDistinct() && aggCall.isDistinct()) {
                JdbcAggregateRule.logger.debug("Distinct used and distinct is not supported by dialect. Aborting pushdown.");
                return false;
            }
            if (aggCall.getAggregation().getKind() == SqlKind.COUNT) {
                JdbcAggregateRule.logger.debug("Evaluating count support.");
                final boolean supportsCount = dialect.supportsCount(aggCall);
                if (!supportsCount) {
                    JdbcAggregateRule.logger.debug("Count operation unsupported. Aborting pushdown.");
                    return false;
                }
                JdbcAggregateRule.logger.debug("Count operation supported.");
            }
            else {
                if (!dialect.supportsAggregateFunction(aggCall.getAggregation().getKind())) {
                    JdbcAggregateRule.logger.debug("Aggregate function {} not supported by dialect. Aborting pushdown.", (Object)aggCall.getAggregation().getName());
                    return false;
                }
                final List<RelDataType> types = (List<RelDataType>)SqlTypeUtil.projectTypes(aggregate.getInput().getRowType(), aggCall.getArgList());
                JdbcAggregateRule.logger.debug("Checking if aggregate function {} used with types {} is supported by dialect using supportsFunction.", (Object)aggCall.getAggregation().getName(), (Object)types);
                if (!dialect.supportsFunction(aggCall, (List)types)) {
                    JdbcAggregateRule.logger.debug("Aggregate {} with type {} not supported by dialect. Aborting pushdown", (Object)aggCall.getAggregation().getName(), (Object)types);
                    return false;
                }
                continue;
            }
        }
        if (!dialect.supportsBooleanAggregation()) {
            JdbcAggregateRule.logger.debug("This dialect does not support boolean aggregations. Verifying no aggregate calls are on booleans.");
            final List<RelDataTypeField> inputRowType = (List<RelDataTypeField>)aggregate.getInput().getRowType().getFieldList();
            for (final AggregateCall aggregateCall : aggregate.getAggCallList()) {
                for (final int argIndex : aggregateCall.getArgList()) {
                    if (inputRowType.get(argIndex).getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    private boolean hasUnpushableTypes(final Aggregate aggregate) {
        final Set<Integer> projectedIndexes = new HashSet<Integer>(aggregate.getGroupSet().asSet());
        for (final AggregateCall aggCall : aggregate.getAggCallList()) {
            projectedIndexes.addAll(aggCall.getArgList());
        }
        final ColumnPropertyAccumulator accumulator = new ColumnPropertyAccumulator();
        aggregate.accept((RelShuttle)accumulator);
        RelNode input = aggregate.getInput();
        while (input.getRowType() == null) {
            if (input instanceof HepRelVertex) {
                input = ((HepRelVertex)input).getCurrentRel();
            }
            else if (input instanceof JdbcCrel) {
                input = ((JdbcCrel)input).getInput();
            }
            else {
                if (!(input instanceof SingleRel)) {
                    continue;
                }
                input = ((SingleRel)input).getInput();
            }
        }
        for (final Integer index : projectedIndexes) {
            if (UnpushableTypeVisitor.hasUnpushableType(input.getRowType().getFieldNames().get(index), accumulator.getColumnProperties())) {
                JdbcAggregateRule.logger.debug("Aggregate has types that are not pushable. Aborting pushdown.");
                return true;
            }
        }
        if (UnpushableTypeVisitor.hasUnpushableTypes(input, input.getChildExps())) {
            JdbcAggregateRule.logger.debug("Aggregate has types that are not pushable. Aborting pushdown.");
            return true;
        }
        return false;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcAggregateRule.class);
        CALCITE_INSTANCE = new JdbcAggregateRule((Class<? extends Aggregate>)LogicalAggregate.class, "JdbcAggregateRuleCrel");
        LOGICAL_INSTANCE = new JdbcAggregateRule((Class<? extends Aggregate>)AggregateRel.class, "JdbcAggregateRuleDrel");
    }
}
