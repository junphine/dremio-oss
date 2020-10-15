package com.dremio.extras.plugins.elastic.planning;

import java.util.*;
import com.dremio.plugins.elastic.planning.*;
import com.dremio.exec.record.*;
import com.dremio.exec.physical.base.*;
import java.util.stream.*;
import com.google.common.collect.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.*;
import com.dremio.common.exceptions.*;

public class ElasticsearchAggregatorGroupScan extends AbstractGroupScan
{
    private final List<ElasticsearchAggExpr> aggregates;
    private final ElasticsearchScanSpec spec;
    private final long rowCountEstimate;
    private final BatchSchema schema;
    
    public ElasticsearchAggregatorGroupScan(final OpProps props, final TableMetadata dataset, final List<ElasticsearchAggExpr> aggregates, final ElasticsearchScanSpec spec, final long rowCountEstimate, final BatchSchema schema) {
        super(props, dataset, (List)GroupScan.ALL_COLUMNS);
        this.aggregates = aggregates;
        this.spec = spec;
        this.rowCountEstimate = rowCountEstimate;
        this.schema = schema;
    }
    
    public List<ElasticsearchAggExpr> getAggregates() {
        return this.aggregates;
    }
    
    public ElasticsearchScanSpec getSpec() {
        return this.spec;
    }
    
    public SubScan getSpecificScan(final List<SplitWork> work) throws ExecutionSetupException {
        final boolean isAllWork = work.size() == this.dataset.getSplitCount();
        final List<SplitAndPartitionInfo> splits = isAllWork ? ImmutableList.of() : (work.stream().map(input -> input.getSplitAndPartitionInfo(true)).collect(Collectors.toList()));
        return new ElasticsearchAggregatorSubScan(this.props, Iterables.getOnlyElement(this.getReferencedTables()), this.getColumns(), this.getAggregates(), this.spec, splits, this.getDataset().getStoragePluginId(), this.props.getSchema());
    }
    
    public int getOperatorType() {
        return 38;
    }
}
