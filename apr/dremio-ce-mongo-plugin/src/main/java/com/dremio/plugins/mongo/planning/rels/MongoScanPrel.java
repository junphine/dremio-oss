package com.dremio.plugins.mongo.planning.rels;

import org.apache.calcite.rel.core.*;
import com.dremio.options.*;
import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import com.dremio.exec.record.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.physical.base.*;
import com.dremio.plugins.mongo.planning.*;
import java.io.*;
import com.dremio.exec.planner.physical.visitor.*;
import java.util.*;
import com.dremio.exec.planner.fragment.*;
import com.dremio.service.namespace.capabilities.*;
import org.slf4j.*;

@Options
public class MongoScanPrel extends TableScan implements LeafPrel, CustomPrel
{
    private static final Logger logger;
    public static final TypeValidators.LongValidator RESERVE;
    public static final TypeValidators.LongValidator LIMIT;
    private final Prel input;
    private MongoScanSpec spec;
    private TableMetadata tableMetadata;
    private List<SchemaPath> columns;
    private List<SchemaPath> sanitizedColumns;
    private BatchSchema schema;
    private double estimatedRowCount;
    private boolean isSingleFragment;
    
    public MongoScanPrel(final RelOptCluster cluster, final RelTraitSet traitSet, final RelOptTable table, final Prel input) {
        super(cluster, traitSet, table);
        this.input = input;
        this.rowType = input.getRowType();
    }
    
    public Prel getOriginPrel() {
        return this.input;
    }
    
    public double estimateRowCount(final RelMetadataQuery mq) {
        return mq.getRowCount((RelNode)this.input);
    }
    
    public RelWriter explainTerms(final RelWriter pw) {
        return super.explainTerms(pw).item("MongoQuery", this.spec.getMongoQuery());
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        return (PhysicalOperator)new MongoGroupScan(creator.props((Prel)this, this.tableMetadata.getUser(), this.schema, MongoScanPrel.RESERVE, MongoScanPrel.LIMIT), this.spec, this.tableMetadata, this.columns, this.sanitizedColumns, this.schema, (long)this.estimatedRowCount, this.isSingleFragment);
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E {
        return (T)logicalVisitor.visitLeaf((LeafPrel)this, value);
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
        return Collections.emptyIterator();
    }
    
    public int getMaxParallelizationWidth() {
        return this.tableMetadata.getSplitCount();
    }
    
    public int getMinParallelizationWidth() {
        return 1;
    }
    
    public DistributionAffinity getDistributionAffinity() {
        return this.tableMetadata.getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY) ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
    }
    
    public void createScanOperator(final MongoScanSpec spec, final TableMetadata tableMetadata, final List<SchemaPath> columns, final List<SchemaPath> sanitizedColumns, final BatchSchema schema, final double estimatedRowCount, final boolean isSingleFragment) {
        this.spec = spec;
        this.tableMetadata = tableMetadata;
        this.columns = columns;
        this.sanitizedColumns = sanitizedColumns;
        this.schema = schema;
        this.estimatedRowCount = estimatedRowCount;
        this.isSingleFragment = isSingleFragment;
    }
    
    static {
        logger = LoggerFactory.getLogger(MongoScanPrel.class);
        RESERVE = (TypeValidators.LongValidator)new TypeValidators.PositiveLongValidator("planner.op.scan.mongo.reserve_bytes", Long.MAX_VALUE, 1000000L);
        LIMIT = (TypeValidators.LongValidator)new TypeValidators.PositiveLongValidator("planner.op.scan.mongo.limit_bytes", Long.MAX_VALUE, Long.MAX_VALUE);
    }
}
