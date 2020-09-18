package com.dremio.plugins.mongo.planning;

import com.dremio.exec.record.*;
import java.util.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.*;
import com.dremio.exec.physical.base.*;
import com.google.common.collect.*;
import com.dremio.common.exceptions.*;
import com.google.common.base.*;

public class MongoGroupScan extends AbstractGroupScan
{
    private static final double[] REDUCTION_FACTOR;
    private final MongoScanSpec spec;
    private final long rowCountEstimate;
    private final boolean isSingleFragment;
    private final BatchSchema schema;
    private final List<SchemaPath> sanitizedColumns;
    
    public MongoGroupScan(final OpProps props, final MongoScanSpec spec, final TableMetadata table, final List<SchemaPath> columns, final List<SchemaPath> sanitizedColumns, final BatchSchema schema, final long rowCountEstimate, final boolean isSingleFragment) {
        super(props, table, (List)columns);
        this.spec = spec;
        this.rowCountEstimate = rowCountEstimate;
        this.isSingleFragment = isSingleFragment;
        this.schema = schema;
        this.sanitizedColumns = sanitizedColumns;
    }
    
    public MongoScanSpec getScanSpec() {
        return this.spec;
    }
    
    public int getMaxParallelizationWidth() {
        return this.isSingleFragment ? 1 : super.getMaxParallelizationWidth();
    }
    
    public SubScan getSpecificScan(final List<SplitWork> work) throws ExecutionSetupException {
        List<MongoSubScanSpec> splitWork;
        if (this.isSingleFragment) {
            splitWork = (List<MongoSubScanSpec>)ImmutableList.of((Object)new MongoSubScanSpec(this.spec.getDbName(), this.spec.getCollectionName(), (List<String>)ImmutableList.of(), null, null, this.spec.getPipeline().copy()));
        }
        else {
            splitWork = (List<MongoSubScanSpec>)FluentIterable.from((Iterable)work).transform((Function)new Function<SplitWork, MongoSubScanSpec>() {
                public MongoSubScanSpec apply(final SplitWork input) {
                    return MongoSubScanSpec.of(MongoGroupScan.this.spec, input.getSplitAndPartitionInfo());
                }
            }).toList();
        }
        return (SubScan)new MongoSubScan(this.props, this.getDataset().getStoragePluginId(), splitWork, (List<SchemaPath>)(this.spec.getPipeline().isSimpleScan() ? GroupScan.ALL_COLUMNS : this.getColumns()), this.sanitizedColumns, this.isSingleFragment, (List<String>)Iterables.getOnlyElement((Iterable)this.getReferencedTables()), this.props.getSchema());
    }
    
    public int getOperatorType() {
        return 37;
    }
    
    public boolean equals(final Object other) {
        if (!(other instanceof MongoGroupScan)) {
            return false;
        }
        final MongoGroupScan castOther = (MongoGroupScan)other;
        return Objects.equal((Object)this.spec, (Object)castOther.spec) && Objects.equal((Object)this.rowCountEstimate, (Object)castOther.rowCountEstimate);
    }
    
    public int hashCode() {
        return Objects.hashCode(new Object[] { this.spec, this.rowCountEstimate });
    }
    
    public String toString() {
        return MoreObjects.toStringHelper((Object)this).add("spec", (Object)this.spec).add("rowCountEstimate", this.rowCountEstimate).toString();
    }
    
    static {
        REDUCTION_FACTOR = new double[] { 1.0, 0.2, 0.1, 0.05 };
    }
}
