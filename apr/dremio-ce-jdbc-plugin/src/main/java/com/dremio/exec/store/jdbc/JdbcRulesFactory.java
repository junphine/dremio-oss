package com.dremio.exec.store.jdbc;

import com.dremio.exec.store.*;
import com.dremio.exec.ops.*;
import com.dremio.exec.planner.*;
import com.dremio.exec.catalog.conf.*;
import java.util.*;
import org.apache.calcite.plan.*;
import com.google.common.collect.*;
import com.dremio.exec.store.jdbc.rules.scan.*;
import com.dremio.exec.store.jdbc.rules.*;
import com.dremio.exec.expr.fn.*;

public class JdbcRulesFactory extends StoragePluginRulesFactory.StoragePluginTypeRulesFactory
{
    public Set<RelOptRule> getRules(final OptimizerRulesContext optimizerContext, final PlannerPhase phase, final SourceType pluginType) {
        switch (phase) {
            case LOGICAL: {
                final ImmutableSet.Builder<RelOptRule> logicalBuilder = ImmutableSet.builder();
                if (optimizerContext.getPlannerSettings().isRelPlanningEnabled()) {
                    return (Set<RelOptRule>)logicalBuilder.add(new JdbcScanCrelRule(pluginType)).build();
                }
            }
            case JDBC_PUSHDOWN:
            case POST_SUBSTITUTION: {
                final ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();
                builder.add(new JdbcScanCrelRule(pluginType));
                builder.add(JdbcExpansionRule.INSTANCE);
                builder.add(JdbcAggregateRule.CALCITE_INSTANCE);
                builder.add(JdbcCalcRule.INSTANCE);
                builder.add(JdbcFilterRule.CALCITE_INSTANCE);
                builder.add(JdbcIntersectRule.INSTANCE);
                builder.add(JdbcJoinRule.CALCITE_INSTANCE);
                builder.add(JdbcMinusRule.INSTANCE);
                builder.add(JdbcProjectRule.CALCITE_INSTANCE);
                builder.add(JdbcSampleRule.CALCITE_INSTANCE);
                builder.add(JdbcSortRule.CALCITE_INSTANCE);
                builder.add(JdbcTableModificationRule.INSTANCE);
                builder.add(JdbcUnionRule.CALCITE_INSTANCE);
                builder.add(JdbcValuesRule.CALCITE_INSTANCE);
                builder.add(JdbcFilterSetOpTransposeRule.INSTANCE);
                return (Set<RelOptRule>)builder.build();
            }
            case RELATIONAL_PLANNING: {
                final ImmutableSet.Builder<RelOptRule> jdbcBuilder = ImmutableSet.builder();
                jdbcBuilder.add(JdbcExpansionRule.INSTANCE);
                jdbcBuilder.add(JdbcAggregateRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcFilterRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcJoinRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcProjectRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcSampleRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcSortRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcUnionRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcValuesRule.LOGICAL_INSTANCE);
                jdbcBuilder.add(JdbcFilterSetOpTransposeRule.INSTANCE);
                jdbcBuilder.add(JdbcLimitRule.INSTANCE);
                jdbcBuilder.add(JdbcSortMergeRule.INSTANCE);
                jdbcBuilder.add(JdbcWindowRule.INSTANCE);
                return (Set<RelOptRule>)jdbcBuilder.build();
            }
            case PHYSICAL: {
                final ImmutableSet.Builder<RelOptRule> physicalBuilder = ImmutableSet.builder();
                physicalBuilder.add(new JdbcPrule((FunctionLookupContext)optimizerContext.getFunctionRegistry()));
                return (Set<RelOptRule>)physicalBuilder.build();
            }
            default: {
                return ImmutableSet.of();
            }
        }
    }
}
