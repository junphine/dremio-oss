package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.planner.sql.handlers.*;
import com.dremio.options.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.expr.fn.*;
import com.google.common.base.*;
import com.dremio.exec.physical.base.*;
import java.io.*;
import com.dremio.exec.record.*;
import com.dremio.exec.planner.physical.visitor.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.*;
import com.dremio.plugins.mongo.planning.rules.*;
import com.dremio.common.expression.*;
import com.dremio.exec.calcite.logical.*;
import java.math.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.plugins.mongo.planning.*;
import java.util.*;
import org.apache.calcite.rex.*;
import org.slf4j.*;

@Options
public class MongoIntermediatePrel extends SinglePrel implements PrelFinalizable, MongoRel
{
    private static final Logger logger;
    public static final TypeValidators.LongValidator RESERVE;
    public static final TypeValidators.LongValidator LIMIT;
    private static final long SAMPLE_SIZE_DENOMINATOR = 10L;
    private final StoragePluginId pluginId;
    private final FunctionLookupContext functionLookupContext;
    private final MongoIntermediateScanPrel scan;
    
    public MongoIntermediatePrel(final RelTraitSet traitSet, final RelNode input, final FunctionLookupContext functionLookupContext, final MongoIntermediateScanPrel scan, final StoragePluginId pluginId) {
        super(input.getCluster(), traitSet, input);
        Preconditions.checkArgument(input.getTraitSet().getTrait((RelTraitDef)ConventionTraitDef.INSTANCE) == MongoConvention.INSTANCE);
        this.input = input;
        this.pluginId = pluginId;
        this.functionLookupContext = functionLookupContext;
        this.scan = scan;
        this.rowType = input.getRowType();
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException("Must be finalized before retrieving physical operator.");
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.size() == 1, "must have one input: %s", inputs);
        return (RelNode)new MongoIntermediatePrel(traitSet, inputs.get(0), this.functionLookupContext, this.scan, this.pluginId);
    }
    
    public MongoIntermediatePrel withNewInput(final MongoRel input) {
        return new MongoIntermediatePrel(input.getTraitSet().replace((RelTrait)Prel.PHYSICAL), (RelNode)input, this.functionLookupContext, this.scan, this.pluginId);
    }
    
    protected Object clone() throws CloneNotSupportedException {
        return this.copy(this.getTraitSet(), this.getInputs());
    }
    
    public MongoIntermediateScanPrel getScan() {
        return this.scan;
    }
    
    public BatchSchema.SelectionVectorMode getEncoding() {
        return BatchSchema.SelectionVectorMode.NONE;
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E {
        throw new UnsupportedOperationException("This needs to be finalized before using a PrelVisitor.");
    }
    
    public boolean needsFinalColumnReordering() {
        return false;
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }
    
    public Prel finalizeRel() {
        final MongoRel mongoTree = (MongoRel)this.getInput().accept((RelShuttle)new MoreRelOptUtil.SubsetRemover());
        final MongoImplementor implementor = new MongoImplementor(this.pluginId);
        final MongoScanSpec spec = implementor.visitChild(0, (RelNode)mongoTree);
        final BatchSchema schema = mongoTree.getSchema(this.functionLookupContext);
        final List<SchemaPath> columns = new ArrayList<SchemaPath>();
        for (final String colNames : this.rowType.getFieldNames()) {
            columns.add(SchemaPath.getSimplePath(colNames));
        }
        final List<SchemaPath> sanitizedColumns = new ArrayList<SchemaPath>();
        for (final String sanitizedColumn : MongoColumnNameSanitizer.sanitizeColumnNames(this.rowType).getFieldNames()) {
            sanitizedColumns.add(SchemaPath.getSimplePath(sanitizedColumn));
        }
        final double estimatedRowCount = this.getCluster().getMetadataQuery().getRowCount((RelNode)this);
        final PlannerSettings settings = (PlannerSettings)this.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class);
        final boolean smallInput = estimatedRowCount < settings.getSliceTarget();
        final boolean isSingleFragment = !settings.isMultiPhaseAggEnabled() || settings.isSingleMode() || smallInput || this.scan.getTableMetadata().getSplitCount() == 1;
        final MongoScanPrel mongoPrel = new MongoScanPrel(this.getCluster(), this.getTraitSet(), this.scan.getTable(), (Prel)mongoTree);
        mongoPrel.createScanOperator(spec, this.scan.getTableMetadata(), columns, sanitizedColumns, schema, estimatedRowCount, isSingleFragment);
        Prel mongoWithLimit;
        if (implementor.hasSample() || implementor.hasLimit()) {
            final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(this.getCluster().getPlanner());
            long fetchSize = implementor.getLimitSize();
            if (implementor.hasSample()) {
                fetchSize = Math.min(fetchSize, SampleCrel.getSampleSizeAndSetMinSampleSize(plannerSettings, 10L));
            }
            mongoWithLimit = PrelUtil.addLimitPrel((Prel)mongoPrel, fetchSize);
        }
        else {
            mongoWithLimit = (Prel)mongoPrel;
        }
        if (!implementor.needsLimitZero()) {
            return mongoWithLimit;
        }
        final RexBuilder b = this.getCluster().getRexBuilder();
        return (Prel)new LimitPrel(this.getCluster(), mongoWithLimit.getTraitSet(), (RelNode)mongoWithLimit, (RexNode)b.makeBigintLiteral(BigDecimal.ZERO), (RexNode)b.makeBigintLiteral(BigDecimal.ZERO));
    }
    
    public MongoScanSpec implement(final MongoImplementor impl) {
        throw new UnsupportedOperationException();
    }
    
    public BatchSchema getSchema(final FunctionLookupContext context) {
        throw new UnsupportedOperationException();
    }
    
    static {
        logger = LoggerFactory.getLogger(MongoIntermediatePrel.class);
        RESERVE = (TypeValidators.LongValidator)new TypeValidators.PositiveLongValidator("planner.op.mongo.reserve_bytes", Long.MAX_VALUE, 1000000L);
        LIMIT = (TypeValidators.LongValidator)new TypeValidators.PositiveLongValidator("planner.op.mongo.limit_bytes", Long.MAX_VALUE, Long.MAX_VALUE);
    }
}
