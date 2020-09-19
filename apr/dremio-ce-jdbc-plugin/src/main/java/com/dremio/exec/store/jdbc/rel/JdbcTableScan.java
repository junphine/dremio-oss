package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import com.google.common.collect.*;
import org.apache.calcite.rel.*;
import java.util.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.tools.RelBuilder;

public class JdbcTableScan extends ScanRelBase implements JdbcRelImpl, Rel
{
    private final List<String> fullPathMinusPluginName;
    private final boolean directNamespaceDescendent;
    
    public JdbcTableScan(final RelOptCluster cluster, final RelTraitSet traitSet, final RelOptTable table, final StoragePluginId pluginId, final TableMetadata tableMetadata, final List<SchemaPath> projectedColumns, final double observedRowcountAdjustment, final boolean directNamespaceDescendent) {
        super(cluster, traitSet, table, pluginId, tableMetadata, (List)projectedColumns, observedRowcountAdjustment);
        this.fullPathMinusPluginName = (List<String>)ImmutableList.copyOf((Iterator)table.getQualifiedName().listIterator(1));
        this.directNamespaceDescendent = directNamespaceDescendent;
    }
    
    public ScanRelBase cloneWithProject(final List<SchemaPath> projection) {
        return new JdbcTableScan(this.getCluster(), this.traitSet, this.table, this.pluginId, this.tableMetadata, projection, this.observedRowcountAdjustment, this.directNamespaceDescendent);
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        assert inputs.isEmpty();
        return (RelNode)new JdbcTableScan(this.getCluster(), traitSet, this.table, this.pluginId, this.tableMetadata, this.getProjectedColumns(), this.observedRowcountAdjustment, this.directNamespaceDescendent);
    }
    
    public boolean isDirectNamespaceDescendent() {
        return this.directNamespaceDescendent;
    }
    
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final JdbcTableScan that = (JdbcTableScan)o;
        return Objects.equals(this.rowType, that.rowType) && Objects.equals(this.getConvention(), that.getConvention());
    }
    
    public int hashCode() {
        return Objects.hash(this.rowType, this.getConvention());
    }
    
    public SqlIdentifier getTableName() {
        return new SqlIdentifier((List)this.fullPathMinusPluginName, SqlParserPos.ZERO);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        return (RelNode)new JdbcTableScan(this.getCluster(), copier.copyOf(this.traitSet), copier.copyOf(this.getTable()), this.pluginId, this.tableMetadata, this.getProjectedColumns(), this.observedRowcountAdjustment, this.directNamespaceDescendent);
    }
    
    public RelNode revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        assert revertedInputs.isEmpty();
        throw new AssertionError("JdbcTableScan should not be reverted");
    }
}
