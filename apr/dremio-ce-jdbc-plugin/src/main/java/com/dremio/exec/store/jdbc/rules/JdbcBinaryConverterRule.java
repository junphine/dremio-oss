package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import org.apache.calcite.tools.*;
import com.dremio.exec.catalog.*;
import java.util.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;

abstract class JdbcBinaryConverterRule extends JdbcConverterRule
{
    JdbcBinaryConverterRule(final Class<? extends RelNode> clazz, final String description) {
        super(operand((Class)clazz, operand((Class)JdbcCrel.class, any()), new RelOptRuleOperand[] { operand((Class)JdbcCrel.class, any()) }), description);
    }
    
    JdbcBinaryConverterRule(final Class<? extends RelNode> clazz, final RelBuilderFactory relBuilderFactory, final String description) {
        super(operand((Class)clazz, operand((Class)JdbcCrel.class, any()), new RelOptRuleOperand[] { operand((Class)JdbcCrel.class, any()) }), relBuilderFactory, description);
    }
    
    protected abstract RelNode convert(final RelNode p0, final JdbcCrel p1, final JdbcCrel p2, final StoragePluginId p3);
    
    public void onMatch(final RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        final JdbcCrel leftCrel = (JdbcCrel)call.rel(1);
        final JdbcCrel rightCrel = (JdbcCrel)call.rel(2);
        final StoragePluginId pluginId = Optional.ofNullable(leftCrel.getPluginId()).orElse(rightCrel.getPluginId());
        final RelNode converted = this.convert(rel, leftCrel, rightCrel, pluginId);
        if (converted != null) {
            final JdbcCrel crel = new JdbcCrel(rel.getCluster(), converted.getTraitSet().replace((RelTrait)Rel.LOGICAL), converted, pluginId);
            call.transformTo((RelNode)crel);
        }
    }
    
    public final boolean matches(final RelOptRuleCall call) {
        if (call.rel(0).getInputs().size() != 2) {
            return false;
        }
        final JdbcCrel leftCrel = (JdbcCrel)call.rel(1);
        final JdbcCrel rightCrel = (JdbcCrel)call.rel(2);
        return leftCrel.getPluginId() == null || rightCrel.getPluginId() == null || (leftCrel.getPluginId().equals(rightCrel.getPluginId()) && this.matchImpl(call));
    }
    
    protected boolean matchImpl(final RelOptRuleCall call) {
        return true;
    }
}
