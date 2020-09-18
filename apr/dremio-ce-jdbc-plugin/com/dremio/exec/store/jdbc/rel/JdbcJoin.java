package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import java.util.*;
import org.apache.calcite.tools.*;

public class JdbcJoin extends Join implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcJoin(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode left, final RelNode right, final RexNode condition, final Set<CorrelationId> variablesSet, final JoinRelType joinType, final StoragePluginId pluginId) {
        super(cluster, traitSet, left, right, condition, (Set)variablesSet, joinType);
        this.pluginId = pluginId;
    }
    
    public JdbcJoin copy(final RelTraitSet traitSet, final RexNode condition, final RelNode left, final RelNode right, final JoinRelType joinType, final boolean semiJoinDone) {
        return new JdbcJoin(this.getCluster(), traitSet, left, right, condition, (Set<CorrelationId>)this.variablesSet, joinType, this.pluginId);
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        final double rowCount = mq.getRowCount((RelNode)this);
        return planner.getCostFactory().makeCost(rowCount, 0.0, 0.0);
    }
    
    public double estimateRowCount(final RelMetadataQuery mq) {
        final double leftRowCount = mq.getRowCount(this.left);
        final double rightRowCount = mq.getRowCount(this.right);
        return Math.max(leftRowCount, rightRowCount);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode left = this.getLeft().accept((RelShuttle)copier);
        final RelNode right = this.getRight().accept((RelShuttle)copier);
        return (RelNode)new JdbcJoin(this.getCluster(), this.getTraitSet(), left, right, copier.copyOf(this.getCondition()), this.getVariablesSet(), this.getJoinType(), this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public Join revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return (Join)builder.push((RelNode)revertedInputs.get(0)).push((RelNode)revertedInputs.get(1)).join(this.joinType, this.condition, (Set)this.variablesSet).build();
    }
}
