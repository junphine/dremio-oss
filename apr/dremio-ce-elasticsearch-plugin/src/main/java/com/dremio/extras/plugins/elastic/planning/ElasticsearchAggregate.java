package com.dremio.extras.plugins.elastic.planning;

import com.dremio.exec.catalog.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.util.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.type.*;
import com.dremio.exec.physical.base.*;
import java.io.*;
import com.dremio.exec.planner.physical.visitor.*;
import com.dremio.exec.record.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.common.logical.data.*;
import java.util.*;
import com.dremio.exec.expr.*;
import com.dremio.plugins.elastic.planning.rels.*;
import org.slf4j.*;

class ElasticsearchAggregate extends Aggregate implements ElasticsearchPrel, ElasticTerminalPrel
{
    private static final Logger logger;
    private StoragePluginId pluginId;
    
    public ElasticsearchAggregate(final RelOptCluster cluster, final RelTraitSet traits, final RelNode child, final boolean indicator, final ImmutableBitSet groupSet, final List<ImmutableBitSet> groupSets, final List<AggregateCall> aggCalls, final StoragePluginId pluginId) {
        super(cluster, traits, child, indicator, groupSet, (List)groupSets, (List)aggCalls);
        this.pluginId = pluginId;
    }
    
    public Aggregate copy(final RelTraitSet relTraitSet, final RelNode relNode, final boolean b, final ImmutableBitSet immutableBitSet, final List<ImmutableBitSet> list, final List<AggregateCall> list1) {
        return new ElasticsearchAggregate(this.getCluster(), relTraitSet, relNode, b, immutableBitSet, list, list1, this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
    
    public RelDataType deriveRowType() {
        return deriveRowType(this.getCluster().getTypeFactory(), this.getInput().getRowType(), this.indicator, this.groupSet, (List)this.groupSets, this.aggCalls);
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> prelVisitor, final X value) throws E {
        return (T)prelVisitor.visitPrel((Prel)this, value);
    }
    
    public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
        return BatchSchema.SelectionVectorMode.DEFAULT;
    }
    
    public BatchSchema.SelectionVectorMode getEncoding() {
        return BatchSchema.SelectionVectorMode.NONE;
    }
    
    public boolean needsFinalColumnReordering() {
        return false;
    }
    
    public Iterator<Prel> iterator() {
        return (Iterator<Prel>)PrelUtil.iter(new RelNode[] { this.getInput() });
    }
    
    public BatchSchema getSchema(final FunctionLookupContext context) {
        final ElasticsearchPrel child = (ElasticsearchPrel)this.getInput();
        final List<NamedExpression> keys = (List<NamedExpression>)RexToExpr.groupSetToExpr((RelNode)child, this.groupSet);
        final List<NamedExpression> aggExprs = (List<NamedExpression>)RexToExpr.aggsToExpr(this.getRowType(), (RelNode)child, this.groupSet, this.aggCalls);
        final BatchSchema childSchema = child.getSchema(context);
        final List<NamedExpression> exprs = new ArrayList<NamedExpression>();
        exprs.addAll(keys);
        exprs.addAll(aggExprs);
        return ExpressionTreeMaterializer.materializeFields((List)exprs, childSchema, context).setSelectionVectorMode(childSchema.getSelectionVectorMode()).build();
    }
    
    public ScanBuilder newScanBuilder() {
        return new AggregateScanBuilder();
    }
    
    static {
        logger = LoggerFactory.getLogger(ElasticsearchAggregate.class);
    }
}
