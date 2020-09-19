package com.dremio.plugins.mongo.planning;

import com.dremio.exec.store.*;
import com.dremio.exec.ops.*;
import com.dremio.exec.planner.*;
import com.dremio.exec.catalog.conf.*;
import java.util.*;
import org.apache.calcite.plan.*;
import com.google.common.collect.*;
import com.google.common.base.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.plugins.mongo.planning.rules.*;

public class MongoRulesFactory extends StoragePluginRulesFactory.StoragePluginTypeRulesFactory
{
    private static final ImmutableList<RuleWithOption> PHYSICAL_RULES;
    
    public Set<RelOptRule> getRules(final OptimizerRulesContext context, final PlannerPhase phase, final SourceType type) {
        final RuleWithOption.OptionPredicate predicate = new RuleWithOption.OptionPredicate(context.getPlannerSettings().getOptions());
        switch (phase) {
            case LOGICAL: {
                return ImmutableSet.of(MongoScanDrule.INSTANCE);
            }
            case PHYSICAL: {
                return FluentIterable.from((Iterable)MongoRulesFactory.PHYSICAL_RULES).filter((Predicate)predicate).transform(Functions.identity()).append((Object[])new RelOptRule[] { new MongoScanPrule((FunctionLookupContext)context.getFunctionRegistry()) }).toSet();
            }
            default: {
                return ImmutableSet.of();
            }
        }
    }
    
    static {
        PHYSICAL_RULES = ImmutableList.of(MongoFilterRule.INSTANCE, MongoProjectRule.INSTANCE, MongoLogicalSortRule.INSTANCE);
    }
}
