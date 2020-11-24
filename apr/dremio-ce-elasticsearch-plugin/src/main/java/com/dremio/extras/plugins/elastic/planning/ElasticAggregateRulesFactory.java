package com.dremio.extras.plugins.elastic.planning;

import com.dremio.plugins.elastic.planning.*;
import com.dremio.exec.ops.*;
import com.dremio.exec.planner.*;
import com.dremio.exec.catalog.conf.*;
import java.util.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.*;
import com.google.common.collect.*;
import com.dremio.exec.expr.fn.*;

public class ElasticAggregateRulesFactory extends ElasticRulesFactory
{
    public Set<RelOptRule> getRules(final OptimizerRulesContext optimizerContext, final PlannerPhase phase, final SourceType pluginType) {
        if (phase == PlannerPhase.PHYSICAL && optimizerContext.getPlannerSettings().getOptions().getOption(ExecConstants.ELASTIC_RULES_AGGREGATE)) {
            final ImmutableSet.Builder<RelOptRule> rules = ImmutableSet.builder();
            rules.addAll(super.getRules(optimizerContext, phase, pluginType));
            rules.add(new ElasticAggregateRule(optimizerContext.getFunctionRegistry()));
            return rules.build();
        }
        return super.getRules(optimizerContext, phase, pluginType);
    }
}
