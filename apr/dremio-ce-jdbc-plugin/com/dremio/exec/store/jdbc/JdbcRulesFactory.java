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
                final ImmutableSet.Builder<RelOptRule> logicalBuilder = (ImmutableSet.Builder<RelOptRule>)ImmutableSet.builder();
                if (optimizerContext.getPlannerSettings().isRelPlanningEnabled()) {
                    return (Set<RelOptRule>)logicalBuilder.add((Object)new JdbcScanCrelRule(pluginType)).build();
                }
            }
            case JDBC_PUSHDOWN:
            case POST_SUBSTITUTION: {
                final ImmutableSet.Builder<RelOptRule> builder = (ImmutableSet.Builder<RelOptRule>)ImmutableSet.builder();
                builder.add((Object)new JdbcScanCrelRule(pluginType));
                builder.add((Object)JdbcExpansionRule.INSTANCE);
                builder.add((Object)JdbcAggregateRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcCalcRule.INSTANCE);
                builder.add((Object)JdbcFilterRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcIntersectRule.INSTANCE);
                builder.add((Object)JdbcJoinRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcMinusRule.INSTANCE);
                builder.add((Object)JdbcProjectRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcSampleRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcSortRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcTableModificationRule.INSTANCE);
                builder.add((Object)JdbcUnionRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcValuesRule.CALCITE_INSTANCE);
                builder.add((Object)JdbcFilterSetOpTransposeRule.INSTANCE);
                return (Set<RelOptRule>)builder.build();
            }
            case RELATIONAL_PLANNING: {
                final ImmutableSet.Builder<RelOptRule> jdbcBuilder = (ImmutableSet.Builder<RelOptRule>)ImmutableSet.builder();
                jdbcBuilder.add((Object)JdbcExpansionRule.INSTANCE);
                jdbcBuilder.add((Object)JdbcAggregateRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcFilterRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcJoinRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcProjectRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcSampleRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcSortRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcUnionRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcValuesRule.LOGICAL_INSTANCE);
                jdbcBuilder.add((Object)JdbcFilterSetOpTransposeRule.INSTANCE);
                jdbcBuilder.add((Object)JdbcLimitRule.INSTANCE);
                jdbcBuilder.add((Object)JdbcSortMergeRule.INSTANCE);
                jdbcBuilder.add((Object)JdbcWindowRule.INSTANCE);
                return (Set<RelOptRule>)jdbcBuilder.build();
            }
            case PHYSICAL: {
                final ImmutableSet.Builder<RelOptRule> physicalBuilder = (ImmutableSet.Builder<RelOptRule>)ImmutableSet.builder();
                physicalBuilder.add((Object)new JdbcPrule((FunctionLookupContext)optimizerContext.getFunctionRegistry()));
                return (Set<RelOptRule>)physicalBuilder.build();
            }
            default: {
                return (Set<RelOptRule>)ImmutableSet.of();
            }
        }
    }
}
