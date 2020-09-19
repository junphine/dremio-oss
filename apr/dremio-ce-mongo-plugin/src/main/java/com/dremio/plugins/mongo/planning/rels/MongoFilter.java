package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.*;
import com.dremio.plugins.mongo.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.rex.*;
import org.bson.*;
import com.dremio.plugins.mongo.planning.rules.*;
import com.dremio.service.namespace.capabilities.*;
import com.dremio.exec.record.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.physical.base.*;
import java.io.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.planner.physical.visitor.*;
import java.util.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.expr.fn.*;

public class MongoFilter extends FilterRelBase implements MongoRel
{
    private final boolean needsCollation;
    
    public MongoFilter(final RelTraitSet traits, final RelNode child, final RexNode condition, final boolean needsCollation) {
        super((Convention)traits.getTrait((RelTraitDef)ConventionTraitDef.INSTANCE), child.getCluster(), traits, child, condition);
        assert this.getConvention() instanceof MongoConvention;
        this.needsCollation = needsCollation;
    }
    
    public MongoScanSpec implement(final MongoImplementor impl) {
        final MongoScanSpec childSpec = impl.visitChild(0, this.getInput());
        final SourceCapabilities capabilities = impl.getPluginId().getCapabilities();
        final boolean isMongo32Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
        final boolean isMongo34Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
        final boolean isMongo36Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_6_FEATURES);
        final BatchSchema schema = (this.getInput() instanceof MongoIntermediateScanPrel) ? ((MongoIntermediateScanPrel)this.getInput()).getBatchSchema() : null;
        if (isMongo36Enabled) {
            final FindQueryGenerator generator = new FindQueryGenerator(schema, this.getInput().getRowType());
            final Document filterToAdd = new Document(MongoPipelineOperators.MATCH.getOperator(), this.condition.accept((RexVisitor)generator));
            return childSpec.plusPipeline(Collections.singletonList(filterToAdd), this.needsCollation);
        }
        final Object filterExpr = this.condition.accept((RexVisitor)new MatchExpressionConverter(schema, this.getInput().getRowType(), isMongo32Enabled, isMongo34Enabled));
        final Document filterToAdd = new Document(MongoPipelineOperators.MATCH.getOperator(), filterExpr);
        return childSpec.plusPipeline(Collections.singletonList(filterToAdd), this.needsCollation);
    }
    
    public Filter copy(final RelTraitSet traitSet, final RelNode input, final RexNode condition) {
        return (Filter)new MongoFilter(traitSet, input, condition, this.needsCollation);
    }
    
    protected Object clone() throws CloneNotSupportedException {
        return this.copy(this.getTraitSet(), this.getInputs());
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E {
        return (T)logicalVisitor.visitPrel((Prel)this, value);
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
        final MongoRel child = (MongoRel)this.getInput();
        return child.getSchema(context);
    }
}
