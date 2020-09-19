package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import java.math.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.dremio.exec.planner.physical.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.logical.*;

public final class JdbcSampleRule extends JdbcUnaryConverterRule
{
    private static final long SAMPLE_SIZE_DENOMINATOR = 5L;
    public static final JdbcSampleRule CALCITE_INSTANCE;
    public static final JdbcSampleRule LOGICAL_INSTANCE;
    
    private JdbcSampleRule(final Class<? extends SampleRelBase> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final JdbcCrel crel = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = crel.getPluginId();
        return pluginId == null || JdbcConverterRule.getDialect(pluginId).supportsSort(true, true);
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final SampleRelBase sample = (SampleRelBase)rel;
        final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(sample.getCluster().getPlanner());
        final RexBuilder rexBuilder = sample.getCluster().getRexBuilder();
        return (RelNode)new JdbcSort(rel.getCluster(), sample.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), RelCollations.EMPTY, (RexNode)rexBuilder.makeBigintLiteral(BigDecimal.ZERO), (RexNode)rexBuilder.makeBigintLiteral(BigDecimal.valueOf(SampleCrel.getSampleSizeAndSetMinSampleSize(plannerSettings, 5L))), pluginId);
    }
    
    static {
        CALCITE_INSTANCE = new JdbcSampleRule((Class<? extends SampleRelBase>)SampleCrel.class, "JdbcSampleRuleCrel");
        LOGICAL_INSTANCE = new JdbcSampleRule((Class<? extends SampleRelBase>)SampleRel.class, "JdbcSampleRuleDrel");
    }
}
