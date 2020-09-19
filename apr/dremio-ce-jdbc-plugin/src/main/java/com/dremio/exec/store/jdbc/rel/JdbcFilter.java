package com.dremio.exec.store.jdbc.rel;

import com.google.common.collect.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import java.util.*;
import org.apache.calcite.tools.*;

public class JdbcFilter extends Filter implements JdbcRelImpl
{
    private final boolean foundContains;
    private final ImmutableSet<CorrelationId> variablesSet;
    private final StoragePluginId pluginId;
    
    public JdbcFilter(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final RexNode condition, final Set<CorrelationId> variablesSet, final StoragePluginId pluginId) {
        super(cluster, traitSet, input, condition);
        this.pluginId = pluginId;
        boolean foundContains = false;
        for (final RexNode rex : this.getChildExps()) {
            if (MoreRelOptUtil.ContainsRexVisitor.hasContains(rex)) {
                foundContains = true;
                break;
            }
        }
        this.foundContains = foundContains;
        this.variablesSet = (ImmutableSet<CorrelationId>)ImmutableSet.copyOf((Collection)variablesSet);
    }
    
    public Set<CorrelationId> getVariablesSet() {
        return (Set<CorrelationId>)this.variablesSet;
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        if (this.foundContains) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        return super.computeSelfCost(planner, mq);
    }
    
    public JdbcFilter copy(final RelTraitSet traitSet, final RelNode input, final RexNode condition) {
        return new JdbcFilter(this.getCluster(), traitSet, input, condition, (Set<CorrelationId>)this.variablesSet, this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        return (RelNode)new JdbcFilter(copier.getCluster(), this.getTraitSet(), input, copier.copyOf(this.getCondition()), (Set<CorrelationId>)this.variablesSet, this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public Filter revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return (Filter)builder.push((RelNode)revertedInputs.get(0)).filter(new RexNode[] { this.condition }).build();
    }
}
