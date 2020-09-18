package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.planner.common.*;
import com.dremio.common.expression.*;
import org.apache.calcite.rex.*;
import java.util.concurrent.*;
import com.dremio.exec.store.jdbc.legacy.*;
import java.util.*;
import org.slf4j.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;

public final class JdbcFilterRule extends JdbcUnaryConverterRule
{
    private static final Logger logger;
    public static final JdbcFilterRule CALCITE_INSTANCE;
    public static final JdbcFilterRule LOGICAL_INSTANCE;
    
    private JdbcFilterRule(final Class<? extends Filter> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        return convert((Filter)rel, crel, pluginId);
    }
    
    public static RelNode convert(final Filter filter, final JdbcCrel crel, final StoragePluginId pluginId) {
        return (RelNode)new JdbcFilter(filter.getCluster(), filter.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), filter.getCondition(), filter.getVariablesSet(), pluginId);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        return matches((Filter)call.rel(0), (JdbcCrel)call.rel(1));
    }
    
    public static boolean matches(final Filter filter, final JdbcCrel crel) {
        try {
            if (UnpushableTypeVisitor.hasUnpushableTypes((RelNode)filter, filter.getCondition())) {
                JdbcFilterRule.logger.debug("Filter has types that are not pushable. Aborting pushdown.");
                return false;
            }
            if (MoreRelOptUtil.ContainsRexVisitor.hasContains(filter.getCondition())) {
                return false;
            }
            final StoragePluginId pluginId = crel.getPluginId();
            if (pluginId == null) {
                return true;
            }
            final RuleContext ruleContext = JdbcConverterRule.getRuleContext(pluginId, crel.getCluster().getRexBuilder());
            final JdbcDremioSqlDialect dialect = JdbcConverterRule.getDialect(pluginId);
            final boolean hasNoBitSupport = !dialect.supportsLiteral(CompleteType.BIT);
            for (final RexNode node : filter.getChildExps()) {
                if (!(boolean)ruleContext.getSupportedExpressions().get((Object)node) || !(boolean)ruleContext.getSubqueryHasSamePluginId().get((Object)node)) {
                    return false;
                }
                if (hasNoBitSupport && dialect.hasBooleanLiteralOrRexCallReturnsBoolean(node, true)) {
                    JdbcFilterRule.logger.debug("Boolean literal used in filter when dialect doesn't support booleans. Aborting pushdown.");
                    return false;
                }
            }
            return true;
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Failure while trying to evaluate pushdown.", e);
        }
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcFilterRule.class);
        CALCITE_INSTANCE = new JdbcFilterRule((Class<? extends Filter>)LogicalFilter.class, "JdbcFilterRuleCrel");
        LOGICAL_INSTANCE = new JdbcFilterRule((Class<? extends Filter>)FilterRel.class, "JdbcFilterRuleDrel");
    }
}
