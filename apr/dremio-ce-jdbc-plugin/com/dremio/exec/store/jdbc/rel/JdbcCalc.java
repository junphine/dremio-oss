package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.util.*;
import org.apache.calcite.rel.metadata.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;

public class JdbcCalc extends SingleRel implements JdbcRelImpl
{
    private final RexProgram program;
    private final StoragePluginId pluginId;
    
    public JdbcCalc(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final RexProgram program, final StoragePluginId pluginId) {
        super(cluster, traitSet, input);
        this.program = program;
        this.rowType = program.getOutputRowType();
        this.pluginId = pluginId;
    }
    
    public JdbcCalc(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final RexProgram program, final int flags, final StoragePluginId pluginId) {
        this(cluster, traitSet, input, program, pluginId);
        Util.discard(flags);
    }
    
    public RelWriter explainTerms(final RelWriter pw) {
        return this.program.explainCalc(super.explainTerms(pw));
    }
    
    public double estimateRowCount(final RelMetadataQuery mq) {
        return RelMdUtil.estimateFilteredRows(this.getInput(), this.program, mq);
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        final double dRows = mq.getRowCount((RelNode)this);
        final double dCpu = mq.getRowCount(this.getInput()) * this.program.getExprCount();
        final double dIo = 0.0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return (RelNode)new JdbcCalc(this.getCluster(), traitSet, (RelNode)sole((List)inputs), this.program, this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        return (RelNode)new JdbcCalc(copier.getCluster(), this.getTraitSet(), input, copier.copyOf(this.program), this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public LogicalCalc revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        if (revertedInputs.get(0).getTraitSet().contains((RelTrait)Rel.LOGICAL)) {
            throw new UnsupportedOperationException("Reverting JdbcCalc with logical convention is not supported");
        }
        return LogicalCalc.create((RelNode)revertedInputs.get(0), this.program);
    }
}
