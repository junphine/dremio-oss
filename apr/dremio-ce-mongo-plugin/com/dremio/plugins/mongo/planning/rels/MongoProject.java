package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rel.*;
import com.dremio.plugins.mongo.planning.rules.*;
import com.dremio.plugins.mongo.*;
import org.bson.*;
import org.apache.calcite.rex.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.physical.base.*;
import java.io.*;
import com.dremio.exec.planner.physical.visitor.*;
import com.dremio.exec.record.*;
import java.util.*;
import com.dremio.exec.planner.physical.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.exec.expr.*;

public class MongoProject extends ProjectRelBase implements MongoRel
{
    private final RelDataType sanitizedRowType;
    private final boolean needsCollation;
    
    public MongoProject(final RelTraitSet traits, final RelNode input, final List<? extends RexNode> projects, final RelDataType rowType, final boolean needsCollation) {
        super((Convention)MongoConvention.INSTANCE, input.getCluster(), traits, input, (List)projects, rowType);
        this.sanitizedRowType = MongoColumnNameSanitizer.sanitizeColumnNames(rowType);
        this.needsCollation = needsCollation;
    }
    
    public final RelDataType getSanitizedRowType() {
        return this.sanitizedRowType;
    }
    
    public MongoScanSpec implement(final MongoImplementor impl) {
        final MongoScanSpec childSpec = impl.visitChild(0, this.getInput());
        final boolean isMongo32Enabled = impl.getPluginId().getCapabilities().getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
        final boolean isMongo34Enabled = impl.getPluginId().getCapabilities().getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
        final Document projectDoc = new Document();
        final RelNode input = this.getInput();
        final boolean needsCollation = false;
        final List<String> outputNames = (List<String>)this.getSanitizedRowType().getFieldNames();
        for (int i = 0; i < this.getProjects().size(); ++i) {
            final String outputName = outputNames.get(i);
            final RexNode expr = this.getProjects().get(i);
            if (!(expr instanceof RexInputRef)) {
                throw new IllegalStateException("Mongo aggregation framework support has been removed. Non RexInputRef projection used.");
            }
            final String inputName = MongoColumnNameSanitizer.sanitizeColumnName(input.getRowType().getFieldNames().get(((RexInputRef)expr).getIndex()));
            if (!inputName.equals(outputName)) {
                throw new IllegalStateException(String.format("Mongo aggregation framework support has been removed. InputName=%s, OutputName=%s.", inputName, outputName));
            }
            projectDoc.put(outputName, (Object)1);
        }
        final MongoScanSpec newSpec = childSpec.plusPipeline(Collections.singletonList(new Document(MongoPipelineOperators.PROJECT.getOperator(), (Object)projectDoc)), needsCollation);
        if (newSpec.getPipeline().isOnlyTrivialProjectOrFilter() && input instanceof MongoIntermediateScanPrel && newSpec.getPipeline().getProjectAsDocument().entrySet().size() == input.getTable().getRowType().getFieldCount()) {
            return new MongoScanSpec(newSpec.getDbName(), newSpec.getCollectionName(), newSpec.getPipeline().newWithoutProject());
        }
        return newSpec;
    }
    
    public Project copy(final RelTraitSet traitSet, final RelNode input, final List<RexNode> projects, final RelDataType rowType) {
        return (Project)new MongoProject(traitSet, input, projects, rowType, this.needsCollation);
    }
    
    protected Object clone() throws CloneNotSupportedException {
        return this.copy(this.getTraitSet(), this.getInputs());
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E, Throwable {
        return (T)logicalVisitor.visitPrel((Prel)this, (Object)value);
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
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
    
    public BatchSchema getSchema(final FunctionLookupContext context) {
        final MongoRel child = (MongoRel)this.getInput();
        final BatchSchema childSchema = child.getSchema(context);
        final ParseContext parseContext = new ParseContext(PrelUtil.getSettings(this.getCluster()));
        return ExpressionTreeMaterializer.materializeFields(this.getProjectExpressions(parseContext), childSchema, context).setSelectionVectorMode(childSchema.getSelectionVectorMode()).build();
    }
}
