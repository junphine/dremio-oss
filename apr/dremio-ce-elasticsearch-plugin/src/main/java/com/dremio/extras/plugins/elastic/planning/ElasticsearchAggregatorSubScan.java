package com.dremio.extras.plugins.elastic.planning;

import java.util.*;
import com.dremio.plugins.elastic.planning.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import com.dremio.exec.record.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.exec.physical.base.*;
import com.google.common.collect.*;
import com.dremio.common.exceptions.*;
import com.google.common.base.*;
import com.google.common.base.Objects;
import com.dremio.exec.planner.fragment.*;

@JsonTypeName("elasticsearch-aggregator-subscan")
public class ElasticsearchAggregatorSubScan extends SubScanWithProjection
{
    private final List<ElasticsearchAggExpr> aggregates;
    private final ElasticsearchScanSpec spec;
    private final StoragePluginId pluginId;
    @JsonIgnore
    private List<SplitAndPartitionInfo> splits;
    
    public ElasticsearchAggregatorSubScan(final OpProps props, final List<String> tablePath, final List<SchemaPath> columns, final List<ElasticsearchAggExpr> aggregates, final ElasticsearchScanSpec spec, final List<SplitAndPartitionInfo> splits, final StoragePluginId pluginId, final BatchSchema fullSchema) {
        super(props, fullSchema, tablePath, columns);
        this.aggregates = aggregates;
        this.spec = spec;
        this.splits = splits;
        this.pluginId = pluginId;
    }
    
    public ElasticsearchAggregatorSubScan(@JsonProperty("props") final OpProps props, @JsonProperty("tableSchemaPath") final List<String> tablePath, @JsonProperty("columns") final List<SchemaPath> columns, @JsonProperty("aggregates") final List<ElasticsearchAggExpr> aggregates, @JsonProperty("spec") final ElasticsearchScanSpec spec, @JsonProperty("pluginId") final StoragePluginId pluginId, @JsonProperty("fullSchema") final BatchSchema fullSchema) {
        super(props, fullSchema, tablePath, columns);
        this.aggregates = aggregates;
        this.spec = spec;
        this.pluginId = pluginId;
    }
    
    public boolean mayLearnSchema() {
        return false;
    }
    
    public List<ElasticsearchAggExpr> getAggregates() {
        return this.aggregates;
    }
    
    public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) throws ExecutionSetupException {
        return (PhysicalOperator)new ElasticsearchAggregatorSubScan(this.getProps(), (List<String>)Iterables.getOnlyElement((Iterable)this.getReferencedTables()), this.getColumns(), this.getAggregates(), this.getScanSpec(), this.getSplits(), this.getPluginId(), this.getFullSchema());
    }
    
    @JsonProperty("spec")
    public ElasticsearchScanSpec getScanSpec() {
        return this.spec;
    }
    
    public List<SplitAndPartitionInfo> getSplits() {
        return this.splits;
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public int getOperatorType() {
        return 39;
    }
    
    public boolean equals(final Object other) {
        if (!(other instanceof ElasticsearchAggregatorSubScan)) {
            return false;
        }
        final ElasticsearchAggregatorSubScan castOther = (ElasticsearchAggregatorSubScan)other;
        return Objects.equal(this.aggregates, castOther.aggregates) && Objects.equal(this.spec, castOther.spec) && Objects.equal(this.splits, castOther.splits) && Objects.equal(this.pluginId, castOther.pluginId);
    }
    
    public int hashCode() {
        return Objects.hashCode(new Object[] { this.aggregates, this.spec, this.splits, this.pluginId });
    }
    
    public String toString() {
        return MoreObjects.toStringHelper(this).add("aggregates", this.aggregates).add("spec", this.spec).add("splits", this.splits).add("pluginId", this.pluginId).toString();
    }
    
    public void collectMinorSpecificAttrs(final MinorDataWriter writer) {
        SplitNormalizer.write(this.getProps(), writer, (List)this.splits);
    }
    
    public void populateMinorSpecificAttrs(final MinorDataReader reader) throws Exception {
        this.splits = (List<SplitAndPartitionInfo>)SplitNormalizer.read(this.getProps(), reader);
    }
}
