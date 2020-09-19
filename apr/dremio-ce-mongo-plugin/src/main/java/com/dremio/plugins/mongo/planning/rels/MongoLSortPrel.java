package com.dremio.plugins.mongo.planning.rels;

import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import java.util.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.physical.base.*;
import java.io.*;
import com.dremio.exec.planner.physical.visitor.*;
import com.dremio.exec.record.*;

public class MongoLSortPrel extends Sort implements Prel
{
    public MongoLSortPrel(final RelOptCluster cluster, final RelTraitSet traits, final RelNode child, final RelCollation collation) {
        super(cluster, traits, child, collation);
    }
    
    public Sort copy(final RelTraitSet traitSet, final RelNode newInput, final RelCollation newCollation, final RexNode offset, final RexNode fetch) {
        return new MongoLSortPrel(this.getCluster(), traitSet, newInput, this.collation);
    }
    
    protected Object clone() throws CloneNotSupportedException {
        return this.copy(this.getTraitSet(), this.getInputs());
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return planner.getCostFactory().makeInfiniteCost();
    }
    
    public Iterator<Prel> iterator() {
        throw new UnsupportedOperationException();
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E {
        throw new UnsupportedOperationException();
    }
    
    public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
        throw new UnsupportedOperationException();
    }
    
    public BatchSchema.SelectionVectorMode getEncoding() {
        throw new UnsupportedOperationException();
    }
    
    public boolean needsFinalColumnReordering() {
        throw new UnsupportedOperationException();
    }
}
