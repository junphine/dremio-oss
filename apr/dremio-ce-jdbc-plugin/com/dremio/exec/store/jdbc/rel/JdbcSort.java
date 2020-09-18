package com.dremio.exec.store.jdbc.rel;

import org.apache.calcite.rel.core.*;
import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import java.util.*;
import org.apache.calcite.tools.*;

public class JdbcSort extends Sort implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcSort(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final RelCollation collation, final RexNode offset, final RexNode fetch, final StoragePluginId pluginId) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert this.getConvention() == input.getConvention();
        this.pluginId = pluginId;
    }
    
    public JdbcSort copy(final RelTraitSet traitSet, final RelNode newInput, final RelCollation newCollation, final RexNode offset, final RexNode fetch) {
        return new JdbcSort(this.getCluster(), traitSet, newInput, newCollation, offset, fetch, this.pluginId);
    }
    
    public static boolean isCollationEmpty(final Sort sort) {
        return sort.getCollation() == null || sort.getCollation() == RelCollations.EMPTY;
    }
    
    public static boolean isOffsetEmpty(final Sort sort) {
        return sort.offset == null || (long)((RexLiteral)sort.offset).getValue2() == 0L;
    }
    
    public static boolean isFetchEmpty(final Sort sort) {
        return sort.fetch == null;
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        return (RelNode)new JdbcSort(copier.getCluster(), this.getTraitSet(), input, this.getCollation(), copier.copyOf(this.offset), copier.copyOf(this.fetch), this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public RelNode revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return builder.push((RelNode)revertedInputs.get(0)).sortLimit(this.collation, this.offset, this.fetch).build();
    }
}
