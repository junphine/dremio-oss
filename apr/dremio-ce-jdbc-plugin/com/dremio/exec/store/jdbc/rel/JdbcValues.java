package com.dremio.exec.store.jdbc.rel;

import org.apache.calcite.rel.core.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.type.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import java.util.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.planner.logical.*;

public class JdbcValues extends Values implements JdbcRelImpl
{
    public JdbcValues(final RelOptCluster cluster, final RelDataType rowType, final ImmutableList<ImmutableList<RexLiteral>> tuples, final RelTraitSet traitSet) {
        super(cluster, rowType, (ImmutableList)tuples, traitSet);
    }
    
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        assert inputs.isEmpty();
        return (RelNode)new JdbcValues(this.getCluster(), this.rowType, (ImmutableList<ImmutableList<RexLiteral>>)this.tuples, traitSet);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        return (RelNode)new JdbcValues(copier.getCluster(), copier.copyOf(this.getRowType()), (ImmutableList<ImmutableList<RexLiteral>>)copier.copyOf(this.getTuples()), this.getTraitSet());
    }
    
    public StoragePluginId getPluginId() {
        return null;
    }
    
    public Values revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        assert revertedInputs.isEmpty();
        final LogicalValues values = LogicalValues.create(this.getCluster(), this.rowType, this.tuples);
        if (this.getTraitSet().contains((RelTrait)Rel.LOGICAL)) {
            return (Values)ValuesRel.from(values);
        }
        return (Values)values;
    }
}
