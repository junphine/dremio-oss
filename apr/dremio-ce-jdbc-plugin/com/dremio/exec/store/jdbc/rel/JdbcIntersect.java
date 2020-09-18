package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.*;

public class JdbcIntersect extends Intersect implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcIntersect(final RelOptCluster cluster, final RelTraitSet traitSet, final List<RelNode> inputs, final boolean all, final StoragePluginId pluginId) {
        super(cluster, traitSet, (List)inputs, all);
        assert !all;
        this.pluginId = pluginId;
    }
    
    public JdbcIntersect copy(final RelTraitSet traitSet, final List<RelNode> inputs, final boolean all) {
        return new JdbcIntersect(this.getCluster(), traitSet, inputs, all, this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        return (RelNode)new JdbcIntersect(copier.getCluster(), this.getTraitSet(), copier.visitAll(this.getInputs()), this.all, this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public LogicalIntersect revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        if (revertedInputs.get(0).getTraitSet().contains((RelTrait)Rel.LOGICAL)) {
            throw new UnsupportedOperationException("Reverting JdbcIntersect with logical convention is not supported");
        }
        return LogicalIntersect.create((List)revertedInputs, this.all);
    }
}
