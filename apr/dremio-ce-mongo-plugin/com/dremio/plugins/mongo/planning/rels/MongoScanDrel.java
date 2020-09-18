package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.planner.logical.*;
import java.util.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import org.apache.calcite.rel.*;

public class MongoScanDrel extends ScanRelBase implements Rel
{
    private final List<String> sanitizedColumns;
    
    public MongoScanDrel(final RelOptCluster cluster, final RelTraitSet traitSet, final RelOptTable table, final StoragePluginId pluginId, final TableMetadata tableMetadata, final List<SchemaPath> projectedColumns, final List<String> sanitizedColumns, final double observedRowcountAdjustment) {
        super(cluster, traitSet, table, pluginId, tableMetadata, (List)projectedColumns, observedRowcountAdjustment);
        this.sanitizedColumns = sanitizedColumns;
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return (RelNode)new MongoScanDrel(this.getCluster(), traitSet, this.table, this.pluginId, this.tableMetadata, this.getProjectedColumns(), this.sanitizedColumns, this.observedRowcountAdjustment);
    }
    
    public ScanRelBase cloneWithProject(final List<SchemaPath> projection) {
        return new MongoScanDrel(this.getCluster(), this.traitSet, this.table, this.pluginId, this.tableMetadata, projection, this.sanitizedColumns, this.observedRowcountAdjustment);
    }
}
