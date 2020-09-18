package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;

abstract class JdbcUnaryConverterRule extends JdbcConverterRule
{
    JdbcUnaryConverterRule(final Class<? extends RelNode> clazz, final String description) {
        super(RelOptHelper.some((Class)clazz, RelOptHelper.any((Class)JdbcCrel.class), new RelOptRuleOperand[0]), description);
    }
    
    protected abstract RelNode convert(final RelNode p0, final JdbcCrel p1, final StoragePluginId p2);
    
    public void onMatch(final RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        final JdbcCrel crel = (JdbcCrel)call.rel(1);
        final RelNode converted = this.convert(rel, crel, crel.getPluginId());
        if (converted != null) {
            final JdbcCrel newCrel = new JdbcCrel(rel.getCluster(), converted.getTraitSet().replace((RelTrait)Rel.LOGICAL), converted, crel.getPluginId());
            call.transformTo((RelNode)newCrel);
        }
    }
}
