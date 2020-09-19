package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import com.dremio.mongo.proto.*;
import com.google.common.base.*;
import com.google.protobuf.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.planner.physical.*;
import java.io.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.record.*;
import com.google.common.collect.*;
import com.dremio.plugins.mongo.planning.rules.*;
import org.bson.*;
import com.dremio.exec.physical.base.*;
import com.dremio.plugins.mongo.planning.*;
import java.util.*;
import com.dremio.exec.planner.common.*;

public class MongoIntermediateScanPrel extends ScanPrelBase implements MongoRel
{
    public MongoIntermediateScanPrel(final RelOptCluster cluster, final RelTraitSet traitSet, final RelOptTable table, final TableMetadata tableMetadata, final List<SchemaPath> projectedColumns, final double observedRowcountAdjustment) {
        super(cluster, traits(cluster, table.getRowCount(), tableMetadata.getSplitCount(), traitSet, tableMetadata), table, tableMetadata.getStoragePluginId(), tableMetadata, projectedColumns, observedRowcountAdjustment);
    }
    
    private static RelTraitSet traits(final RelOptCluster cluster, final double rowCount, final int splitCount, final RelTraitSet traitSet, final TableMetadata tableMetadata) {
        final PlannerSettings settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
        final boolean smallInput = rowCount < settings.getSliceTarget();
        MongoReaderProto.MongoTableXattr extendedAttributes;
        try {
            extendedAttributes = MongoReaderProto.MongoTableXattr.parseFrom(tableMetadata.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
        }
        catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate((Throwable)e);
        }
        DistributionTrait distribution;
        if (settings.isMultiPhaseAggEnabled() && !settings.isSingleMode() && !smallInput && splitCount > 1 && extendedAttributes.getType() != MongoReaderProto.CollectionType.SINGLE_PARTITION) {
            distribution = DistributionTrait.ANY;
        }
        else {
            distribution = DistributionTrait.SINGLETON;
        }
        return traitSet.plus((RelTrait)distribution);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return (RelNode)new MongoIntermediateScanPrel(this.getCluster(), traitSet, this.getTable(), this.tableMetadata, this.getProjectedColumns(), this.observedRowcountAdjustment);
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator physicalPlanCreator) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public MongoIntermediateScanPrel cloneWithProject(final List<SchemaPath> projection) {
        return new MongoIntermediateScanPrel(this.getCluster(), this.getTraitSet(), this.table, this.tableMetadata, projection, this.observedRowcountAdjustment);
    }
    
    public BatchSchema getSchema(final FunctionLookupContext context) {
        final LinkedHashSet<SchemaPath> paths = new LinkedHashSet<SchemaPath>();
        for (final SchemaPath p : this.getProjectedColumns()) {
            paths.add(SchemaPath.getSimplePath(p.getRootSegment().getPath()));
        }
        return this.tableMetadata.getSchema().maskAndReorder(ImmutableList.copyOf((Collection)paths));
    }
    
    public MongoScanSpec implement(final MongoImplementor impl) {
        final MongoScanSpec spec = new MongoScanSpec(this.getTableMetadata().getName().getPathComponents().get(1), this.getTableMetadata().getName().getPathComponents().get(2), MongoPipeline.createMongoPipeline(null, false));
        final List<SchemaPath> projectedColumns = (List<SchemaPath>)this.getProjectedColumns();
        if (projectedColumns == null || projectedColumns.equals(GroupScan.ALL_COLUMNS)) {
            return spec;
        }
        final Document columnsDoc = new Document();
        final List<SchemaPath> newColumns = new ArrayList<SchemaPath>();
        for (final SchemaPath col : projectedColumns) {
            final SchemaPath newCol = SchemaPath.getSimplePath(col.getRootSegment().getPath());
            if (!newColumns.contains(newCol)) {
                newColumns.add(newCol);
            }
        }
        for (final SchemaPath col : newColumns) {
            columnsDoc.put(col.getAsUnescapedPath(), 1);
        }
        return spec.plusPipeline(Collections.singletonList(new Document(MongoPipelineOperators.PROJECT.getOperator(), columnsDoc)), false);
    }
}
