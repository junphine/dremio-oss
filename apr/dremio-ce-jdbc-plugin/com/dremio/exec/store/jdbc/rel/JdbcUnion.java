package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.*;

public class JdbcUnion extends Union implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcUnion(final RelOptCluster cluster, final RelTraitSet traitSet, final List<RelNode> inputs, final boolean all, final StoragePluginId pluginId) {
        super(cluster, traitSet, (List)inputs, all);
        this.pluginId = pluginId;
    }
    
    public JdbcUnion copy(final RelTraitSet traitSet, final List<RelNode> inputs, final boolean all) {
        return new JdbcUnion(this.getCluster(), traitSet, inputs, all, this.pluginId);
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public RelNode revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return builder.pushAll((Iterable)revertedInputs).union(this.all).build();
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        return (RelNode)new JdbcUnion(copier.getCluster(), this.getTraitSet(), copier.visitAll(this.getInputs()), this.all, this.pluginId);
    }
}
