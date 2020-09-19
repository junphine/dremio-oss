package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.planner.sql.handlers.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.physical.base.*;
import java.io.*;
import com.dremio.exec.record.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.planner.physical.visitor.*;
import java.util.*;
import com.dremio.exec.planner.fragment.*;

public class JdbcIntermediatePrel extends JdbcRelBase implements LeafPrel, PrelFinalizable
{
    private final FunctionLookupContext context;
    private final StoragePluginId pluginId;
    
    public JdbcIntermediatePrel(final RelOptCluster cluster, final RelTraitSet traits, final RelNode jdbcSubTree, final FunctionLookupContext context, final StoragePluginId pluginId) {
        super(cluster, traits, jdbcSubTree);
        this.context = context;
        this.pluginId = pluginId;
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return (RelNode)new JdbcIntermediatePrel(this.getCluster(), traitSet, this.jdbcSubTree, this.context, this.pluginId);
    }
    
    public BatchSchema.SelectionVectorMode getEncoding() {
        return BatchSchema.SelectionVectorMode.NONE;
    }
    
    public Prel finalizeRel() {
        final List<RexNode> projects = new ArrayList<RexNode>(this.getCluster().getRexBuilder().identityProjects(this.getRowType()));
        final JdbcPrel input = new JdbcPrel(this.getCluster(), this.getTraitSet(), this, this.context, this.pluginId);
        return (Prel)ProjectPrel.create(this.getCluster(), this.getTraitSet(), (RelNode)input, (List)projects, this.rowType);
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E {
        throw new UnsupportedOperationException("This needs to be finalized before using a PrelVisitor.");
    }
    
    public Iterator<Prel> iterator() {
        return Collections.emptyIterator();
    }
    
    public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
        return BatchSchema.SelectionVectorMode.DEFAULT;
    }
    
    public boolean needsFinalColumnReordering() {
        return true;
    }
    
    public int getMaxParallelizationWidth() {
        return 1;
    }
    
    public int getMinParallelizationWidth() {
        return 1;
    }
    
    public DistributionAffinity getDistributionAffinity() {
        return DistributionAffinity.SOFT;
    }
}
