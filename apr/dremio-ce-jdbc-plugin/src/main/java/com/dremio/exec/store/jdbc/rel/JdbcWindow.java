package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.*;
import java.util.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.logical.*;

public class JdbcWindow extends WindowRelBase implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcWindow(final RelOptCluster cluster, final RelTraitSet traits, final RelNode child, final List<RexLiteral> constants, final RelDataType rowType, final List<Window.Group> windows, final StoragePluginId pluginId) {
        super(cluster, traits, child, (List)constants, MoreRelOptUtil.uniqifyFieldName(rowType, cluster.getTypeFactory()), (List)windows);
        this.pluginId = pluginId;
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return (RelNode)new JdbcWindow(this.getCluster(), traitSet, (RelNode)sole((List)inputs), (List<RexLiteral>)this.constants, this.getRowType(), (List<Window.Group>)this.groups, this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        return (RelNode)new JdbcWindow(copier.getCluster(), this.getTraitSet(), input, (List<RexLiteral>)this.constants, copier.copyOf(this.getRowType()), (List<Window.Group>)this.groups, this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public RelNode revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        final RelNode input = revertedInputs.get(0);
        return (RelNode)WindowRel.create(input.getCluster(), input.getTraitSet(), input, (List)this.constants, this.rowType, (List)this.groups);
    }
}
