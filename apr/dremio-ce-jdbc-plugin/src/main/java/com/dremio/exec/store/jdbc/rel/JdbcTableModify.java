package com.dremio.exec.store.jdbc.rel;

import org.apache.calcite.rel.core.*;
import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.prepare.*;
import java.util.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.rel.logical.*;

public class JdbcTableModify extends TableModify implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcTableModify(final RelOptCluster cluster, final RelTraitSet traitSet, final RelOptTable table, final Prepare.CatalogReader catalogReader, final RelNode input, final TableModify.Operation operation, final List<String> updateColumnList, final List<RexNode> sourceExpressionList, final boolean flattened, final StoragePluginId pluginId) {
        super(cluster, traitSet, table, catalogReader, input, operation, (List)updateColumnList, (List)sourceExpressionList, flattened);
        this.pluginId = pluginId;
        final ModifiableTable modifiableTable = (ModifiableTable)table.unwrap((Class)ModifiableTable.class);
        if (modifiableTable == null) {
            throw new AssertionError();
        }
        if (table.getExpression((Class)Queryable.class) == null) {
            throw new AssertionError();
        }
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return (RelNode)new JdbcTableModify(this.getCluster(), traitSet, this.getTable(), this.getCatalogReader(), (RelNode)sole((List)inputs), this.getOperation(), this.getUpdateColumnList(), this.getSourceExpressionList(), this.isFlattened(), this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        return (RelNode)new JdbcTableModify(copier.getCluster(), this.getTraitSet(), copier.copyOf(this.getTable()), this.getCatalogReader(), input, this.getOperation(), this.getUpdateColumnList(), copier.copyRexNodes(this.getSourceExpressionList()), this.isFlattened(), this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public LogicalTableModify revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return LogicalTableModify.create(this.table, this.catalogReader, (RelNode)revertedInputs.get(0), this.getOperation(), this.getUpdateColumnList(), this.getSourceExpressionList(), this.isFlattened());
    }
}
