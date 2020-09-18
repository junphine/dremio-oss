package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.core.*;

public class JdbcMinus extends Minus implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcMinus(final RelOptCluster cluster, final RelTraitSet traitSet, final List<RelNode> inputs, final boolean all, final StoragePluginId pluginId) {
        super(cluster, traitSet, (List)inputs, all);
        assert !all;
        this.pluginId = pluginId;
    }
    
    public JdbcMinus copy(final RelTraitSet traitSet, final List<RelNode> inputs, final boolean all) {
        return new JdbcMinus(this.getCluster(), traitSet, inputs, all, this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        return (RelNode)new JdbcMinus(copier.getCluster(), this.getTraitSet(), copier.visitAll(this.getInputs()), this.all, this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public LogicalMinus revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return LogicalMinus.create((List)revertedInputs, this.all);
    }
}
